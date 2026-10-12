# EG-CONTRACT — Architecture and implementation plan

**State:** PROPOSED. This plan is executable from a fresh public checkout.

## Existing implementation to reuse

| Authority | Existing component | Rule for changes |
|---|---|---|
| Method and errors | `crates/eg-capabilities/src/bin/gen_contract.rs`, Rust `ErrorCode` declarations | Extend declarations and generator; never edit generated JSON as the source. |
| Generated catalogs | `contract/methods.json`, `contract/errors.json`, `contract/schemas/`, `contract/fixtures/` | Check digest and fixture parity after regeneration. |
| Python models and package | `epistemic_graph/generated/models.py`, `epistemic_graph/contract/` | Re-export generated wire classes and verify installed wheel contents. |
| Canonical signing | Rust method codec and `scripts/build_method_codec_wasm.py` | Use its body bytes in Go and JS; do not port canonicalization by hand. |
| Go/JS consumers | `clients/go/`, `clients/js/` | Run vector and error tests against embedded WASM and an installed/served engine. |
| Release checks | `scripts/generated_artifacts.py`, `scripts/check_wheel_completeness.py`, `scripts/check_wheel_privacy.py`, `scripts/ci_parity.sh`, `.github/workflows/release.yml` | Make hosted checks hermetic and source-revision-bound; keep live deployment proof a separate gate. |

## Contract data model

Each method record has a stable `id`, `domain`, `request_schema`, `result_schema`, `policy`, `replay_class`, `stability`, `consumer_profiles`, `is_wire_callable` and `error_set`. Each error record has `code`, `class`, `retryable`, `http_status_hint`. The generator fails if a referenced schema or error is absent, a method ID is duplicated, an allowed wire method has no dispatcher, or a stable code changes meaning without a deliberate compatibility change. The receipt hashes every generated file and identifies the source revision.

The request envelope and signed body are distinct. The codec canonicalizes only the specified body fields. The client attaches identity and transport metadata after encoding. Signatures cover the bytes actually sent. A decoder checks schema before use and maps each error code to its generated typed class. An unknown-code variant supports newer server errors. Unknown fields in strict request/result models still fail closed.

## Work sequence

1. **Catalog audit:** enumerate every Rust-dispatched method and error. Compare generated descriptors to reachable server paths. Correct missing transaction conflict error mapping.
2. **One Python shape:** replace duplicate hand-written DTO definitions with generated re-exports; preserve import compatibility through aliases only where named and tested.
3. **Codec parity:** build Rust WASM from pinned tool versions. Compare both embedded copies. Run Go/JS/Python/Rust vectors and record source and artifact hashes. Reduce size by removing unused decode paths before changing optimization flags.
4. **Package:** include catalogs, schemas and receipt in wheel data. Clean-install into a new virtual environment and verify loader digests and typed operations. Avoid source-tree fallbacks.
5. **CI:** run full root Python tests with the kernel/engine build in hosted CI, static contract reachability, Go/JS tests, all release feature profiles, and differential quality scanners. Tests needing a service start one from checked-in fixtures; deployment/soak has its own documented workflow.
6. **Release:** build the wheel twice in clean environments, compare normalized artifacts, install from the built wheel, run consumer parity and mark a precise commit/tag. Record evidence per requirement.

## Fresh checkout developer path

Clone the public repository; install the versions declared by `rust-toolchain.toml`, `Cargo.lock`, `pyproject.toml`, `clients/go/go.mod` and `clients/js/package-lock.json`. Run the repository's normal generators, followed by:

```bash
cargo run -q --locked -p eg-capabilities --features contract --bin gen_contract -- --check
python3 scripts/build_method_codec_wasm.py --check
go test ./...
npm test
python3 -m pytest -q tests/test_generated_client_contract.py tests/test_contract_method_reachability.py
```

Run Go commands from `clients/go` and npm commands from `clients/js`. If a named Python test is moved, update this plan with its replacement before removing the gate. The complete merge gate is `bash scripts/ci_parity.sh` plus the hosted release workflow. CI must start each integration service from a checked-in container/fixture recipe. The job must tear down each service.

## Design decisions

- Catalog metadata is generated from Rust code because server dispatch and stable errors are EG authority.
- The contract receipt binds all language projections to one revision; a package containing current JSON but stale WASM is invalid.
- Hosted CI may split bounded fast checks from long soak. A missing external environment cannot convert a required public gate into a permanent skip.
- Quality scans are differential over changed production code with explicit tracked configuration, and manual scanners remain observable release evidence.

## Amendments

### 2026-10-11: reconcile the native suite contract census

EG-CONTRACT-R009 and EG-CONTRACT-R015 retain one current method authority. The
domain registry, generated method catalog, capability ledger and both packaged
receipts must agree, including CapabilityCoverage. Receipt regression checks
derive their census from the declared catalog instead of pinning historical
totals. The scanner wiring regression recognizes the blocking conditional
pre-commit step for PR-diff and full-publication checks. It preserves the
intentional pipelines@main workflow policy and pinned scanner binaries.

### 2026-10-11: cache server compilation without bypassing source builds

EG-CONTRACT-R009 retains the unconditional locked, full-featured server build
and same-run revision/digest-verified artifact handoff. The server preparation
job installs a pinned sccache before enabling its compiler wrapper only for
that build step. This addresses the prior missing-wrapper CI incident without
changing local Cargo configuration, wheel builds, or the other validation jobs.
GitHub's built-in cache backend needs no repository secret; only main writes,
and other refs read. Cargo still checks the current source and sccache keys
cacheable compiler outputs by compiler inputs; final binary linking still runs.
Cache statistics make warm reuse observable. The ten-minute target remains
unproven until a main cache seed and a subsequent hosted run are measured.

### 2026-10-11: register the bounded cluster harness tests

EG-CONTRACT-R023.1 owns the completeness regression from EG-DURABLE-KERNEL-R014.2.
Add the exact cluster-harness test path to the existing binary-composition component.
Keep all existing ownership boundaries unchanged. Run the real completeness checker
and pin that exact component assignment in its existing regression module.

### 2026-10-11: retain completed facade compiler outputs across interrupted gates

EG-CONTRACT-R009 extends the same pinned compiler-cache setup to the full-facade
Cargo test step. The server and facade share YAML anchors for installation and
step-local cache settings. Both still run their original Cargo commands;
the later Whisper and bounded-memory tests keep their existing configuration.
Only main writes to the GitHub cache. Each successful cacheable compilation can
write its output before the overall job ends, so an interrupted cold gate does
not discard every completed compilation. This does not cache final test-binary
linking. Hosted cold and warm runs must establish whether the gate meets the
ten-minute target. Test scope, failure propagation, and existing timeouts stay intact.

### 2026-10-11: run workspace gates on the supported hosted runner

EG-CONTRACT-R035 keeps the workspace-crates gate on `ubuntu-latest` for every
release workflow event. The gate requires the declared Cyrus-SASL and curl
build headers, installed by its existing Ubuntu provisioning step. Selecting
the already-supported hosted runner makes that prerequisite reproducible
without changing any self-hosted security policy. Preserve the exact package
installation, test commands, dependency graph, event scope and timeout.
The workflow regression pins hosted placement and unconditional provisioning;
native end-to-end acceptance still requires the hosted job to finish.

### Capability refusal and decoding contracts

EG-CONTRACT-R014 publishes the existing governed-actor refusal and routes it only
to GovernedChange, preserving the fail-closed replicated actor boundary.
EG-IDENTITY-R004 classifies federation:read as an engine user scope. Registry
membership grants no scopes and preserves owner checks and the handler's kg:read
requirement. EG-DECISION-ENGINE-R063 defines operation-selected typed decision-fit
and decision-eval decoding, including nullable status/receipt results and strict
rejection of wrong operation shapes. The native generator owns all contract
artifacts. Refusal, scope-census, wrapper and byte-for-byte freshness guards
validate their consistency.

Diagnostic and unserved-helper prefixes are not additional wire codes.
The census follows a conservative function/method-reference graph from
Response-returning functions
and Response::err boundaries, includes macro literals, and excludes typed
StatisticalErrorCode diagnostic arguments. Inline cfg(test) modules are excluded
structurally. Regressions retain production before and after test modules,
feature-gated production, called helpers and formatted refusal strings, while
excluding an uncalled helper and typed diagnostic text.

### 2026-10-11: keep cache reports on the build backend after daemon loss

EG-CONTRACT-R009 scopes the shared GitHub cache backend settings to the server
and full-facade jobs. The build, explicit statistics, and action cleanup therefore
receive the same backend and main-only write policy. The compiler wrapper stays
on the same two build/test steps. Statistics describe only the current daemon.
A restart resets its counters. This does not prove the earlier daemon wrote nothing. This fixes the reporting configuration mismatch without explaining why
the facade daemon stopped or claiming a runtime improvement.

### 2026-10-11: keep the cache idle window above the bounded validation job

EG-CONTRACT-R009 sets `SCCACHE_IDLE_TIMEOUT=7200` in the existing shared server
and facade backend settings. In pinned sccache 0.16.0, incoming requests reset
the idle timer; an active compilation does not inhibit it. A finite two-hour
window exceeds both jobs' existing maximum of 60 minutes and retains eventual
idle shutdown on persistent runners. The job deadlines, test scope, wrapper
scope, and main-only cache writes remain unchanged. This prevents the documented
idle-shutdown hazard within a fresh job daemon's lifetime. It does not prove the
historical EOF cause or establish a facade speedup. Statistics synthesize zero
counters from caller configuration when no daemon is reachable; they do not
start a replacement daemon.

### 2026-10-11: provision the manifest toolchain before lint formatting

The lint job must install the manifest-selected toolchain and rustfmt before either formatting step, per EG-CONTRACT-R031. Reuse the workflow's manifest-resolved rustup installation
pattern rather than duplicating its channel pin or relying on the hosted image.
The workflow regression checks unconditional provisioning and step ordering for
both PR-scoped and full-publication formatting. This repairs runner setup only;
full hosted quality-gate and runtime acceptance remain independently required.

R031.1 owns only the runner setup regression. R031.2 preserves the outstanding complete same-tree census and independent clippy/formatting evidence.
A setup-only test cannot qualify the broad parent requirement.

### Facade build timing and empty helper harnesses

EG-CONTRACT-R009 disables the default unit-test harness only for four single-file
helper binaries with no tests. These helpers observe RF-019, migrate shards,
restore backups, and export lake fixtures. Their ordinary binaries still build
for integration coverage. The server harness, library tests, and every
integration target remain enabled. A source guard requires revisiting the
exemption if a helper gains tests or a source module.

The mandatory full-facade command adds stable Cargo `--timings`. Clear only
previous timing reports after cache restore. Upload the current report on
`always()` with the existing pinned artifact action and a source/run/attempt
name. A canceled build may produce no report. Report its absence without
claiming a successful test run. This adds evidence for the next natural build.
Profiles, timeouts, cache permissions, and test failure propagation stay intact.
Saved build time and the sub-ten-minute goal remain unmeasured.

### 2026-10-11: conservative local source-gate scope

The contract-method reachability and durable-table registration scanners retain
their full algorithms. Automatic commit/push wrappers may omit a scan only when
complete raw Git changes and supplied filenames independently prove ordinary
documentation-only files, with verified index/worktree or clean push state.
The existing CI documentation predicate is reused without broadening its default;
only the local wrapper also recognizes the exact development skill document.
CI, direct manual hooks, all-files, deletions and uncertain state run full scans.
This is hook plumbing under R024, not completion of the full R023 gate suite.

Frozen commit-stage `--from-ref`/`--to-ref` checks use the same immutable-ref,
clean tracked worktree and exact checked-out-tip validation as push. Either range variable requires a valid complete pair.
Partial or invalid ranges never fall back to the staged-document optimization. Ordinary commits without
range variables retain index/worktree agreement checks.

### 2026-10-12: preserve governed-change refusal codes

EG-CONTRACT-R014 declares every existing GovernedRefusal through ServerErrorCode.
The producer uses those typed declarations, and the generator publishes their
GovernedChange method routes. Unknown codes still become INTERNAL without raw
diagnostic text. EG-DURABLE-KERNEL-R027.2 retains two-person approval, direct
identity and exact-scope checks. The forged-actor regression requires the exact
authorization refusal instead of accepting any governed prefix.

### 2026-10-12: current architecture boundaries

EG-CONTRACT-R052 documents the current source composition. It proposes no component split, feature change, or release change.
The reviewed source revision is `54aab65786bf72fac94f565796eb21f5d1ec72c4`; all paths below refer to that revision unless updated with new evidence.

#### Model: module owners and dependencies

[Cargo.toml](../../Cargo.toml) owns workspace membership and feature edges.
Derive the inventory by parsing `workspace.members`, then include the root package once.
Expand member globs and honor exclusions if the manifest introduces them.
Do not freeze the current member count into an acceptance test.
Each group below describes responsibility; it does not declare a new deployable unit.

| Responsibility | Existing modules |
|---|---|
| Composition, listeners, policy, persistence adapters | Root `src/` and its `epistemic-graph` library and executable targets |
| Shared records, policies, codecs, resources | `eg-types`, `eg-capabilities`, `eg-method-codec`, `eg-resource` |
| Physical storage, mutation authority, graph state | `eg-storage`, `eg-transaction`, `eg-core`, `eg-durable`, `eg-sqlite-format` |
| Query and semantic processing | `eg-query`, `eg-plan`, `eg-rdf`, `eg-shacl`, `eg-shex`, `eg-graphql`, `eg-epistemic` |
| Compute and indexed data | `eg-compute`, `eg-numeric`, `eg-ann`, `eg-geo`, `eg-tensor`, `eg-stream`, `eg-tsdb`, `eg-text`, `eg-kvcache`, `eg-lake` |
| Governed media contracts and processing | `eg-modality`, `eg-document`, `eg-image`, `eg-audio`, `eg-video`, `eg-alignment`, `eg-asr-whisper`, `eg-tts-piper` |
| Jobs, programs, state machines, extensions | `eg-jobs`, `eg-program`, `eg-statechart`, `eg-wasm`, `eg-pyengine` |
| Native visualization | `eg-viz-core`, `eg-viz-columnstore`, `eg-viz-kernels`, `eg-viz-export`, `eg-viz-graph-tiles` |
| Optional quantum processing | `eg-quantum-core`, `eg-quantum-gates`, `eg-quantum-sim`, `eg-quantum-hardware`, `eg-quantum-jobs`, `eg-quantum-workloads` |

The root composes workspace libraries. Lower contracts do not depend on the root facade.
For example, `eg-types` has no internal dependency; `eg-modality` and `eg-capabilities` depend on `eg-types`.
`eg-storage` depends on `eg-types`; `eg-transaction` depends on both.
`eg-core` uses graph primitives and optional storage/transaction adapters. `eg-query` and `eg-plan` compose those capabilities.
These examples are manifest observations, not a strict hierarchy for every crate.
Read normal, optional, target-specific, build, and test dependency sections separately when checking an edge.

[The scanner configuration](../../.config/arch-lint.toml) explicitly defines no dependency-direction rule.
Existing scanner success therefore does not prove this dependency direction.
[The component registry](../../architecture/component-registry.yml) remains `draft_unreviewed`; its older measurements do not qualify this map.
The converged-engine design remains owned by [EG-UNIFIED-DATA-PLANE-R001](../unified-data-plane/requirements.md).

#### Behavior: builds, packages, and deployments

| Boundary | Current source contract | Evidence limit |
|---|---|---|
| Cargo selection | Root `default` includes `full`; `full` includes native visualization. Quantum remains optional. | Old comments excluding visualization do not override feature values. |
| Optional layers | `cluster` extends `full`; `full-extras` extends `full` for additional integrations. | Compilation does not prove listener activation or a deployed cluster. |
| Server executable | The server has a separate no-Python-extension dependency check. | That boundary does not prohibit extensions elsewhere in the wheel. |
| Release wheel | [folded-wheel/action.yml](../../.github/actions/folded-wheel/action.yml) builds the server, `eg-numeric/python`, and `eg-pyengine/python,security`. | The recipe requires both kernels through `check_wheel_completeness.py --require-engine-kernel`. |
| Source-build backend | [build_backend.py](../../build_backend.py), `_compose`, folds the numeric kernel at the reviewed revision. | It does not establish parity with the release action's engine-kernel composition. |
| Python extras | [pyproject.toml](../../pyproject.toml) defines `full = []` and `numeric = []`. | These aliases neither change the compiled engine nor select a deployment shape. |
| Runtime shape | A served engine and the embedded binding expose distinct process boundaries. | Configuration, identity, persistence, and runtime checks determine usable capabilities. |

EG-C-07/08, EG-T-10, EG-CONTRACT-R033, and EG-CONTRACT-R037 retain artifact and feature-profile acceptance.
EG-CONTRACT-R047 already owns rolling-wheel distribution. Package size does not imply a new architectural boundary.
The source-backend composition difference belongs to that existing packaging acceptance; this document does not repair or qualify it.

#### Wiring: shared contracts and call sites

| Contract | Existing authority and source route | Owning requirements |
|---|---|---|
| Request identity and policy | `eg-types` identity records feed server access checks; `eg-capabilities::policy` declares method policy. Generated clients project the contract. | [EG-IDENTITY](../engine-identity-and-scope/requirements.md); EG-C-01/02 |
| Artifact identity and provenance | `eg-modality` exports `OpaqueRef`, `ArtifactBundle`, `EvidenceLocus`, and `Provenance`. `eg-alignment` relates evidence locations. | [EG-FEDERATED-QUERY](../federated-query-and-reasoning/requirements.md), especially R039.1; native media coverage remains separate. |
| Epistemic provenance | `eg-epistemic` operates on graph claims and provenance; `eg-plan` exposes `KnowledgeSet` and knowledge batches. | [EG-FEDERATED-QUERY](../federated-query-and-reasoning/spec.md) reasoning and retrieval requirements |
| Physical ownership and mutation | `eg-storage` owns physical access; `eg-transaction::MutationKernel` owns admission, ordering, replay, and commit capabilities. | [EG-DURABLE-KERNEL](../durable-graph-kernel/requirements.md), including R019, R021, and R034 |
| Served mutation | `src/server/mutation.rs:136` exports `commit_mutation`; conditional and coalesced routes retain their own entry paths. | [EG-DURABLE-KERNEL](../durable-graph-kernel/spec.md); EG-C-02 error projection |
| Planned query | `src/server/handlers/query/planning.rs:424`, `run_unified_with`, binds execution through the existing planner. | [FQR-03/04/06](../federated-query-and-reasoning/spec.md), R038–R055 |
| Direct native access | [engine.md](../../docs/architecture/engine.md) shows dispatch reaching graph state both directly and through planning. | [EG-DURABLE-KERNEL-R032/R037](../durable-graph-kernel/requirements.md) |

Repository scope is not RBAC scope: `engine-identity-and-scope` owns authorization identity.
Artifact references, principals, graph records, replay identities, and query plans remain distinct typed contracts.
A shared store does not imply one global snapshot or an unbounded transaction across all stores.
R034 retains its cross-store crash and anomaly acceptance. A scope model alone does not satisfy that acceptance.
Federation retains capability-checked pushdown, exact residuals, source grants, budgets, and the shared outbound guard.
This map adds no planner, identity registry, provenance store, or transaction authority.

#### Refusal and evidence

Reject architecture claims that have only a module name, feature flag, diagram arrow, or generated status as support.
Use `status.json` for generator-owned delivery state. Preserve requirement acceptance and public receipts independently.
`VERIFIED` identifies a landed row with a bound test; it does not establish a successful current deployment.
EG-T-12 defines documentation checks. Existing owner tests retain all required served, negative, recovery, and release observations.

#### Diagram coverage and owner follow-ons

[The engine diagram](../../docs/assets/engine-architecture.svg) groups responsibilities; it does not establish conformance for every labeled interface.
The following map covers its five pillars, interface band, planner/mutation band, and durable-store band.
Owner links identify existing definitions. Missing clauses identify documentation work, not absent implementation.
Consult each owner's generated `status.json` for current delivery state and its test specification for acceptance.

| Diagram label | Existing definition owner | Coverage boundary and next definition |
|---|---|---|
| Connected data: graph, RDF, relational, vector, text | [EG-FEDERATED-QUERY](../federated-query-and-reasoning/requirements.md), R038–R055; [EG-UNIFIED-DATA-PLANE](../unified-data-plane/requirements.md), R001 and source contracts | Preserve exact results, residuals, grants, and bounded federation. Native query conformance needs its own clauses. |
| Reasoning: OWL, SHACL, ShEx, claims, belief, provenance | [EG-FEDERATED-QUERY](../federated-query-and-reasoning/spec.md) | ShEx lacks an explicit requirement across the reviewed specs. Use `eg-shex` and existing reasoning dispatch as source. |
| Analytics: SQL, graph, vector, numeric, programs | [EG-FINANCE-PRIMITIVES](../finance-primitives/requirements.md), [EG-DECISION-ENGINE](../decision-engine/requirements.md), and [EG-FEDERATED-QUERY](../federated-query-and-reasoning/requirements.md) | Separate numeric kernels, UQL programs R027/R065, native `ProgramOptimize`, and EG-DECISION-ENGINE-R135 compiled-run replay. |
| Signals and time: events, streams, metrics, bitemporal state, CDC | [EG-FEDERATED-QUERY](../federated-query-and-reasoning/requirements.md), R023; [EG-DURABLE-KERNEL](../durable-graph-kernel/requirements.md), R078–R081 | Stream grammar does not define event-time, window, or watermark lifecycle. Bitemporal served requirements still need their specified evidence. |
| Multimodal: document, image, audio, video, blobs, evidence | [EG-FEDERATED-QUERY](../federated-query-and-reasoning/requirements.md), R039.1; [EG-UNIFIED-DATA-PLANE](../unified-data-plane/requirements.md), R040 | Four-modality retrieval differs from native lifecycle. R040 names text/image/audio; video promotion needs an explicit reconciliation. |
| Authenticated Python interface | EG-C-01–EG-C-05 and EG-C-10; [EG-IDENTITY](../engine-identity-and-scope/requirements.md) | Preserve installed-client, signed-body, strict-decoding, and served authorization evidence. |
| UQL, SQL, SPARQL interfaces | [EG-FEDERATED-QUERY](../federated-query-and-reasoning/spec.md); [EG-UNIFIED-DATA-PLANE](../unified-data-plane/requirements.md) | Coverage of a dialect or foreign adapter does not qualify every native interface operation. |
| Cypher, GraphQL, Bolt interfaces | EG-C-01/02 and EG-CONTRACT-R033 for projection; [EG-FEDERATED-QUERY](../federated-query-and-reasoning/requirements.md) for semantics | R053 and kernel R024.3 cover foreign Cypher. R073 and decision R064 cover other GraphQL uses. Bolt lacks an explicit requirement. |
| Streams interface | [EG-FEDERATED-QUERY-R023](../federated-query-and-reasoning/requirements.md) and [EG-DURABLE-KERNEL](../durable-graph-kernel/requirements.md) | Existing owners must define stream lifecycle acceptance. Parser coverage does not prove runtime behavior. |
| Unified planner and governed mutations | [FQR-03/04/06](../federated-query-and-reasoning/spec.md); [EG-DURABLE-KERNEL-R034/R043](../durable-graph-kernel/requirements.md) | Prove common policy, authority, and state across interfaces. Preserve direct native paths and cross-modal atomicity acceptance. |
| Authoritative durable store | [EG-DURABLE-KERNEL](../durable-graph-kernel/spec.md); [EG-IDENTITY](../engine-identity-and-scope/spec.md) | Commit-before-ack, audit, isolation, and optional replication retain their existing owner acceptance. |
| Read-through recovery | [EG-DURABLE-KERNEL](../durable-graph-kernel/requirements.md) | R039 rebuild and federation R036 index reopening do not explicitly prove safe eviction followed by a durable read. |

Keep each follow-on in its existing owner and split it into five-minute definition children.
Each child must state Model, Behavior, Wiring, Refusal, and Evidence before implementation.
For runtime rows, name the actual production call site and its served integration proof.

| Follow-on owner | Source to reuse | Narrow definition sequence |
|---|---|---|
| Federated query and reasoning | `src/server/bolt_wire/`, native Cypher handlers, `crates/eg-graphql/`, `crates/eg-shex/` | Define each native interface separately. Include GraphQL mutations, pagination, federation, and authenticated subscriptions where source supports them. |
| Federated query and reasoning | [modality_serving.md](../../docs/architecture/modality_serving.md) and `crates/eg-modality/` | Define native ingest, typed predicates, concurrent updates and removals, cold restoration, and evidence-location contracts separately. |
| Decision engine and federated query | [native-program-optimization.md](../../docs/architecture/native-program-optimization.md), `crates/eg-program/` | Confirm one semantic owner for `ProgramOptimize`; link the other owner. Preserve UQL and compiled-run contracts separately. |
| Unified data plane | R040 and federation R039.1 | Resolve video promotion scope without silently expanding the existing acceptance. |
| Federated query and durable kernel | `crates/eg-stream/`, `crates/eg-tsdb/`, existing stream dispatch | Define runtime windows, watermarks, state recovery, and refusal independently of the stream grammar. |
| Durable graph kernel | [engine.md](../../docs/architecture/engine.md), `crates/eg-core/src/read_through.rs`, `src/redb_store/` | Add safe-eviction/read-through acceptance under the existing durability owner, with cache-miss and restart observations. |

No follow-on above asserts a passed test or changes a subsystem's acceptance in this batch.

### 2026-10-12: source-build composition after engine parity

EG-CONTRACT-R052.2 records this source update at
`3756ef27dd49e18aa54b962c7a1c743504b34dd6`.
The architecture review above remains pinned to
`54aab65786bf72fac94f565796eb21f5d1ec72c4`; its numeric-only backend
observation describes that earlier revision.

[build_backend.py](../../build_backend.py), `_compose`, now builds and folds
`eg-numeric/python` and `eg-pyengine/python,security` with the existing
`inject_numeric_kernel.py` and `inject_pyengine.py` helpers.
Both `build_wheel` and `build_editable` use this composition path.
The staging lock covers the server build, both component builds, and both folds.
The server binary retains its separate no-PyO3 boundary.

This closes the source-composition difference recorded above.
EG-C-07/08, EG-T-10, EG-CONTRACT-R033, and EG-CONTRACT-R037 retain their
exact-artifact and feature-profile acceptance.
Source wiring alone does not qualify native release artifacts or promote
the architecture requirement's generated status.

<!-- spec-definition-patch:owner-batch-20261012-public-engine-contract-and-release -->
## Reviewed residual closure plan — owner batch 2026-10-12

### `EG-CONTRACT-R001.1` — AU:109

- Existing EG parent/rollup: `EG-CONTRACT-R001`; preserve its existing obligations.
- Original objective: Agents use typed models as their primary data model, with construction, validation and serialization throughout the agent lifecycle.
- Original intake fields (exact source record array): ["AU-HARNESS","Agents use typed models as their primary data model, with construction, validation and serialization throughout the agent lifecycle.","mechanism","(blank)","prompts/conversations/old/pydantic-native.txt","LOW","WRITE"]
- Contract definition: EG SHALL export strict generated request, result, identity, and decision models, including their complete nested type closure, with a manifest containing the contract revision, schema digest, and exported type identifiers. Construction, deserialization, and serialization SHALL use the same pinned generated schema. Unknown nested fields, invalid enum values, incompatible revisions, and invalid scalar types SHALL fail with a field path before dispatch or persistence. AU owns AgentLifecycleState and its transition rules; its state composes these generated EG types with HarnessRequest, phase, tool requests/results, and RunOutcome. EG SHALL NOT define a second AU lifecycle state machine or permit an unvalidated dictionary to replace an admitted generated value. Rehydration SHALL validate the persisted schema revision and every nested generated value before another agent step can run.
- Production wiring and owner boundary: Extend the existing EG contract generator and its generated-model manifest; consume those exports in AU HarnessRequest/run_agent and the proposed AU-owned AgentLifecycleState. Tests must cross the generated-client and actual AU execution/persistence boundaries.
- Planned acceptance: Proposed integration case lifecycle_generated_types_survive_run_and_restart: the generated client enters the real AU run_agent path, transitions through tool request and result to RunOutcome, persists state, restarts, and resumes using the same typed values. Unknown nested fields, wrong scalar types, stale schema revisions, and malformed persisted values each refuse before the next step; AU separately refuses an illegal phase transition. Read back the persisted and resumed values independently and compare every field. A generated-model unit round trip alone does not pass.
- Preserve existing coverage: `AU-HARNESS-R007`, `EG-CONTRACT-R001`, `AU-HARNESS-R008`.
- Source record: `AU:109` SHA-256 `c0d92285a141dcaac6817bcac25ffa438517afeba50be716211751f3a37a31c2`. Owner references: epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/public-engine-contract-and-release/requirements.md:5 (EG-CONTRACT-R001); agent-utilities@50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/harness-evolution/requirements.md:12 (AU-HARNESS-R007); agent-utilities@50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/harness-evolution/requirements.md:13 (AU-HARNESS-R008).
- Explicit owner holds: AU-HARNESS-R007.40 is unmerged. Lifecycle completion requires the AU-owned state, transitions, persistence, and real consumer integration; this EG export clause alone cannot close AU:109.
- Unmerged consumer dependencies: AU-HARNESS-R007.40 (draft/unmerged).
- Proposed owner-review choices, not source-established facts: A schema/type manifest is a proposed export contract, not evidence of a current implemented manifest..
- Keep status SPECIFIED with empty landed_in/verified_by until separately reviewed implementation and served evidence exist; no receipt is inferred.

### `EG-CONTRACT-R001.2` — AU:121

- Existing EG parent/rollup: `EG-CONTRACT-R001`; preserve its existing obligations.
- Original objective: Prompt JSON blueprints and Pydantic models round-trip through disk with stable formatting and no field lost; a round-trip difference fails the test.
- Contract definition: EG SHALL bind each exported prompt blueprint to its blueprint/schema revision, generated-model revision, semantic digest, and canonical-byte digest. The versioned JSON formatting profile SHALL use UTF-8 without a BOM, sorted object keys, two-space indentation, LF line endings, one final newline, explicit declared default and null fields, preserved array order, and no NaN or Infinity. Parsing SHALL reject unknown fields and scalar coercion. The schema-defined number representation and Unicode scalar values SHALL survive without rounding or normalization. Export, SDK-owned disk write, disk read, strict model validation, and re-export SHALL preserve both the typed field values and the canonical bytes. A mismatch SHALL report the first differing JSON pointer/value or byte offset and fail acceptance. EG owns blueprint semantics and validation; existing SDK/GraphOS governed source-effect contracts own filesystem effects.
- Production wiring and owner boundary: Use EG-TYPED-PACKS-R094.2/.3/.5/.7: the served agent admin export/import and durable persistence path, with SDK-GOVERNED-WRITEBACK-R004.5-.12 and GRAPHOS-FLEET-R003.4-.10 for effects. Reuse those contracts rather than adding an EG filesystem writer.
- Planned acceptance: Proposed integration case blueprint_canonical_disk_roundtrip_has_no_difference: use the served R094 export and governed SDK disk write/read, then import and re-export after restart. Include nested defaults, nulls, ordered arrays, Unicode, and boundary numeric values admitted by the schema. Compare exact canonical bytes plus schema-typed values. Lost fields, unknown fields, coerced numbers, changed array order, and a changed final newline must each fail the difference oracle. Capture the source-effect receipt; a serializer-only test is insufficient.
- Preserve existing coverage: `EG-CONTRACT-R001`, `EG-TYPED-PACKS-R094.2`, `EG-TYPED-PACKS-R094.5`, `EG-TYPED-PACKS-R094.7`, `EG-TYPED-PACKS-R094.3`.
- Source record: `AU:121` SHA-256 `98b7ad1186ec2a385b334ffa0ad1b28888157ed6b51c6ed4172eeb7b7ef6768e`. Owner references: epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/public-engine-contract-and-release/requirements.md:5 (EG-CONTRACT-R001); epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/typed-packs-and-catalog/requirements.md:149 (EG-TYPED-PACKS-R094.2); epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/typed-packs-and-catalog/requirements.md:150 (EG-TYPED-PACKS-R094.3); epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/typed-packs-and-catalog/requirements.md:152 (EG-TYPED-PACKS-R094.5); epistemic-graph@e1db3f4359e659dc07fb0e0ba5e021bed40c9f26:specs/typed-packs-and-catalog/requirements.md:154 (EG-TYPED-PACKS-R094.7); agent-utilities@50c313ab01871ff7e10aee2a9d0254fa55d71f07:specs/agent-control-plane/requirements.md:30 (AU-CONTROL-R025).
- Explicit owner holds: AU-CONTROL-R025.40 remains unmerged. The formatter must pin an exact number encoder/profile version; no claim that arbitrary JSON parsers preserve numeric spelling is permitted.
- Unmerged consumer dependencies: AU-CONTROL-R025.40 (draft/unmerged).
- Proposed owner-review choices, not source-established facts: The explicit formatting profile matches the AU draft proposal; it is a proposed cross-owner contract, not an existing landed receipt..
- Keep status SPECIFIED with empty landed_in/verified_by until separately reviewed implementation and served evidence exist; no receipt is inferred.

<!-- /spec-definition-patch -->
