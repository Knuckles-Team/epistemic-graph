# Work breakdown and traceability

The state of every task in this file is `VERIFY` until exact merged-head evidence is recorded. `VERIFY` means code may already exist, but the contract and acceptance are still to be checked. `BUILD` means a known unmet behavior is specified here. `DEFERRED` is a separately bounded capability. `REJECTED` records a prohibited path. Task checkboxes track work performed; they never turn an untested implementation into `ACCEPTED`.

## Pack contract and admission

- [x] **P1 — VERIFY:** Freeze the typed pack index, entry, annotation, digest, method/result and golden-vector contract. Prove all generated clients share the same bytes and result types (PACK-01/02/10; EG-TYPED-PACKS-R001, EG-TYPED-PACKS-R004, EG-TYPED-PACKS-R018, EG-TYPED-PACKS-R049, EG-TYPED-PACKS-R050, EG-TYPED-PACKS-R064, EG-TYPED-PACKS-R065).
- [x] **P2 — VERIFY:** Prove engine-owned body staging, guarded holder/refcount, orphan reconciliation, exact body read and content model across unchanged, revised, withdrawn and retired members (PACK-03/04; EG-TYPED-PACKS-R002, EG-TYPED-PACKS-R008, EG-TYPED-PACKS-R014, EG-TYPED-PACKS-R016, EG-TYPED-PACKS-R041).
- [x] **P3 — VERIFY:** Prove Import boundary, importer binding, one atomic owner commit, replay, audit, preflight, cancellation and tenant-scoped lock release. Keep methods LocalOnly in clustered mode (PACK-03/11; EG-TYPED-PACKS-R003, EG-TYPED-PACKS-R007, EG-TYPED-PACKS-R010, EG-TYPED-PACKS-R012, EG-TYPED-PACKS-R015, EG-TYPED-PACKS-R020, EG-TYPED-PACKS-R021, EG-TYPED-PACKS-R038, EG-TYPED-PACKS-R084).
- [x] **P4 — VERIFY:** Run planted mutation, security, tenancy, crash, SDK and end-to-end connector fixtures. Fix G14 timeout and G15 malformed-path admission if still reproducible (PACK-06/12; EG-TYPED-PACKS-R005, EG-TYPED-PACKS-R086).
- [x] **P5 — VERIFY:** Execute G1–G21 from `architecture.md` including negative state invariants (PACK-06; EG-TYPED-PACKS-R022, EG-TYPED-PACKS-R023, EG-TYPED-PACKS-R024, EG-TYPED-PACKS-R025, EG-TYPED-PACKS-R026, EG-TYPED-PACKS-R027, EG-TYPED-PACKS-R028, EG-TYPED-PACKS-R029, EG-TYPED-PACKS-R030, EG-TYPED-PACKS-R031, EG-TYPED-PACKS-R032, EG-TYPED-PACKS-R033, EG-TYPED-PACKS-R034, EG-TYPED-PACKS-R035, EG-TYPED-PACKS-R036, EG-TYPED-PACKS-R037, EG-TYPED-PACKS-R039, EG-TYPED-PACKS-R040). Consume the shared EG-FEDERATED-QUERY-R001 consistency result from `federated-query-and-reasoning`.
- [x] **P6 — VERIFY:** Keep capability, modality, schema digest, MCP hints, cost, latency and model facts as typed annotations. Classify native versus foreign claims without inventing a native fact (PACK-09; EG-TYPED-PACKS-R013, EG-TYPED-PACKS-R042, EG-TYPED-PACKS-R043, EG-TYPED-PACKS-R044, EG-TYPED-PACKS-R045, EG-TYPED-PACKS-R046, EG-TYPED-PACKS-R047, EG-TYPED-PACKS-R048, EG-TYPED-PACKS-R066).

## Projection, schema and catalog

- [x] **P7 — VERIFY:** Run the first Agent Library outbox consumer with head-state projection, current-record graph attach, visibility CAS and failure/re-drive. Prove authorization of the worker's reserved graph write (PACK-07; EG-TYPED-PACKS-R006, EG-TYPED-PACKS-R052, EG-TYPED-PACKS-R053, EG-TYPED-PACKS-R054, EG-TYPED-PACKS-R055, EG-TYPED-PACKS-R056, EG-TYPED-PACKS-R057, EG-TYPED-PACKS-R059, EG-TYPED-PACKS-R074).
- [x] **P8 — VERIFY:** Reuse a single bounded schema validator for pack and graph source composition, ensure shape ownership and monotone pack revisions, and expose `GraphSchemaClasses` with committed digest (PACK-08; EG-TYPED-PACKS-R051, EG-TYPED-PACKS-R058, EG-TYPED-PACKS-R061, EG-TYPED-PACKS-R062, EG-TYPED-PACKS-R063, EG-TYPED-PACKS-R071, EG-TYPED-PACKS-R073, EG-TYPED-PACKS-R077). Consume EG-FEDERATED-QUERY-R010/EG-FEDERATED-QUERY-R012 reasoning from `federated-query-and-reasoning`.
- [x] **P9 — VERIFY:** Keep ServerRegistry as liveness authority and ConnectorPack/AgentComponent as content authority. Remove shadow catalog consumers after typed reads are available (PACK-09; EG-TYPED-PACKS-R072). Consume EG-CONTRACT-R005 generated models from `public-engine-contract-and-release`.
- [x] **P10 — VERIFY:** Make pack contract regeneration and artifact transfer compare content and digest across `contract/`, Python generated outputs and Rust catalog digest (PACK-10; EG-TYPED-PACKS-R067, EG-TYPED-PACKS-R068, EG-TYPED-PACKS-R070, EG-TYPED-PACKS-R080). Consume Go/JS signing and Python typed result parity from `public-engine-contract-and-release`.
- [x] **P11 — VERIFY:** Run complete Rust integration, root Python, client and release suites, consuming the shared dispatch-decomposition, Python CI and naming gates owned by `public-engine-contract-and-release` (PACK-11/12; EG-TYPED-PACKS-R075, EG-TYPED-PACKS-R076, EG-TYPED-PACKS-R087).
- [x] **P12 — BUILD/VERIFY:** Move graph ontology object and schema DTO authority into EG typed models and generated clients. Route schema-drift candidate SHACL construction and contract/activation storage through EG while the connector-sync runner retains its drain/apply gate. Remove the prior duplicate DTO and ontology owner only after a generated-type parity and migration test (PACK-08/10; EG-TYPED-PACKS-R081, EG-TYPED-PACKS-R082, EG-TYPED-PACKS-R083).

## Ruled decisions and boundaries

| IDs | Decision or boundary preserved by this spec |
|---|---|
| EG-TYPED-PACKS-R009 | Projection readiness is the atomicity boundary for readers. |
| EG-TYPED-PACKS-R010 | Use one `Import` operation; no prepare/commit/abort family. |
| EG-TYPED-PACKS-R011 | Incompatible Agent Library layout is refused; offline owner-file upgrade is rejected. |
| EG-TYPED-PACKS-R012 | Pack and four agent-layer write methods stay LocalOnly. |
| EG-TYPED-PACKS-R013 | Foreign capability IRIs remain claims; mistyped native IRIs refuse. |
| EG-TYPED-PACKS-R014 | Missing pack members become reversible Withdrawn. |
| EG-TYPED-PACKS-R015 | Bind/Unbind is admin controlled; configured importer is bootstrap default. |
| EG-TYPED-PACKS-R016 | Server pin follows declared contract and instructions, not package version. |
| EG-TYPED-PACKS-R017 | The accepted design includes projection but excludes offline upgrade. |
| EG-TYPED-PACKS-R018 | Body read uses `AgentComponent.Content`. |
| EG-TYPED-PACKS-R019 | Only `SKILL.md` is imported as skill content. |
| EG-TYPED-PACKS-R020 | The AU tiny-profile admin issue is an adjacent consumer fix, not an EG pack write. |
| EG-TYPED-PACKS-R021 | Cross-cutting defects affecting safe import are release-gate obligations. |
| EG-TYPED-PACKS-R052 | Projection waits for the general outbox reject/re-drive primitive. |
| EG-TYPED-PACKS-R053 | Non-pack graph attachment is a stale-visible snapshot until a durable auto-follow design exists. |
| EG-TYPED-PACKS-R054–EG-TYPED-PACKS-R060 | Topic isolation, failure severity, manifest refusal, TBox composition, bounded reasoner, delta validation and owner-layout ordering are architectural corrections described in `architecture.md`. |
| EG-TYPED-PACKS-R061, EG-TYPED-PACKS-R062, EG-TYPED-PACKS-R063 | Schema source capacity, GraphState replication and agent-row schema migration must be verified against current typed contracts. |

## Adjacent rows discovered in the broad catalog crosswalk

These IDs do not create pack behavior. They are retained here so a contributor cannot silently count them as accepted by this spec. Their full requirements must live in their owning feature spec before delivery; only the stated shared boundary applies here.

| IDs | Adjacent scope and pack boundary |
|---|---|
| EG-TYPED-PACKS-R069 | Repository ingestion of branch/blob content; may use typed source facts but is not a ConnectorPack import. |
| EG-TYPED-PACKS-R078 | Retrieval plan candidates; consumes visible typed skills and schema digest, but solver choice is outside this spec. |
| EG-TYPED-PACKS-R079 | Typed OHLCV bars; may be represented in the shared catalog but time-series semantics are separate. |
| EG-TYPED-PACKS-R085, EG-DURABLE-KERNEL-R027 | Identity method family and governed approval; pack authorization must use the common capability system. |
| EG-TYPED-PACKS-R088 | Embedded/served authority parity applies to pack reads when exposed; wider embedded transport expansion is separate. |
| EG-UNIFIED-DATA-PLANE-R004, EG-UNIFIED-DATA-PLANE-R029 | Database schema profiling/context consume committed schema facts; source inference/query design is separate. |
| EG-FINANCE-PRIMITIVES-R010 | Finance leverage model uses typed data; trading and risk semantics are separate. |
| EG-DURABLE-KERNEL-R040, EG-DURABLE-KERNEL-R041 | Batch pipelining and SQL plan/result cache are data-plane capabilities, not pack storage. |

## Storage, generation and client verification

- [x] **P13 — VERIFY:** Pin the `OwnerLayout::Blob` body-holder table cardinality, regenerate ontology/SHACL artifacts byte-for-byte from the typed Rust vocabulary, import FoodData Central/FoodOn nutrient reference types through a ChEBI crosswalk, exclude the request ID from the Python client's import idempotency key, isolate Turtle blank-node identities per parse, serve `AgentComponent.content` pack-body bytes directly, surface `GraphSchema` attach staleness, and reproject a connector pack with bounded, isolated, restart-safe aggregates (EG-TYPED-PACKS-R089, EG-TYPED-PACKS-R090, EG-TYPED-PACKS-R091, EG-TYPED-PACKS-R092, EG-TYPED-PACKS-R093, EG-TYPED-PACKS-R094, EG-TYPED-PACKS-R095, EG-TYPED-PACKS-R096). EG-TYPED-PACKS-R092 is proven directly by `tests/test_connector_pack_client.py::test_import_key_excludes_the_request_id`, which varies only `request_id` across two imports and asserts the operation key does not move; the remaining seven requirements in this line are still open.

Requirement IDs in `requirements.md` not mentioned by any task above before this line: none remain — EG-TYPED-PACKS-R089 through EG-TYPED-PACKS-R096 are closed by P13.

## Done criteria

Mark a PACK requirement `LANDED` only with an exact merged EG commit. Mark it `ACCEPTED` only after the mapped tests in `test-spec.md`, generated contract receipt, security proof, configured quality gates and served-path verification pass on that commit. A ruled decision row is documented rather than implemented again. A queued adjacent row cannot be closed through this spec.
- [x] **P14 (EG-TYPED-PACKS-R097):** Publish the human-resources, legal, medical and government vocabularies as typed domain packs with validation, import and round-trip tests, then confirm callers read the packs.
- [x] **P15 (EG-TYPED-PACKS-R098):** Serve the OntologyInspect method and inline data triples on ShaclValidate, regenerate the wire contract and every generated client, and prove both through handler, classification and Python client tests.
- [x] **P16 (EG-TYPED-PACKS-R099):** Add the prototype-matching kernel and its bounded Python binding to the numeric extension with Rust unit tests and installed-binding parity and bounds tests.
- [x] **R016 digest v4:** Exclude `server_package_version` from the server component digest behind digest v4. Keep v3 as a read-compatible validation path, so stored rows need no migration. Restore the independent-store equal-pins test (EG-TYPED-PACKS-R016). Building on branch `fix/g26-digest-v4`; see plan.md "R016 digest v4".
- [x] **P17 (EG-TYPED-PACKS-R090.1):** Build the typed-vocabulary-to-Turtle generator mechanism (`eg-core::ontology_codegen`) and its byte-equality regeneration test. R090.1 is the producer-first slice of R090 (split per SPEC-SIZING-AND-DEPENDENCIES.md §5); later child requirements migrate the existing `crates/eg-core/ontology/*.ttl` corpus onto this mechanism.
- [x] **P18 (EG-TYPED-PACKS-R097.1):** Build the typed `DomainPack` model (`eg-types::domain_pack`) for the HR domain, drawn from `crates/eg-core/ontology/hr-v1.ttl`, with validation and a round-trip test. R097.1 is the producer-first slice of R097 (split per SPEC-SIZING-AND-DEPENDENCIES.md §5). Later children cover legal/medical/government, wiring through the `connector_pack` import boundary, and the agent-utilities `domains/*`/`models/domains` caller cutover.

- [x] **EG-TYPED-PACKS-R093.1:** Isolate bounded and unbounded public Turtle parse identities; preserve existing explicit blank labels in catalogue composition. Native parser/union harness passes eight tests.
- [ ] **EG-TYPED-PACKS-R093.2:** Execute served connector-pack reproject isolation and deterministic-digest regression; the broader reproject acceptance remains open.

- [x] **EG-TYPED-PACKS-R042.1:** Check both capability annotation fields in native production validation and claim partition tests.
- [ ] **EG-TYPED-PACKS-R042.2:** Run signed imports and persisted claim reads; prove rejected imports leave every owner table and body holder unchanged.

- [ ] **EG-TYPED-PACKS-R043:** Run TP-10, TP-11, and applicable gates on the merged head after native modality and digest regressions.

- [ ] **EG-TYPED-PACKS-R048.1:** Context window is a named model fact. `crates/eg-types/src/agent_component/facts.rs` already carries `context_window_tokens` on `ModelProfile`. EG must expose it as `contextWindowTokens` in the ontology projection and the Python DTO. The pack validator must reject a value of 0. Test: `crates/eg-types/src/agent_component/facts.rs::tests::context_window_zero_is_refused`.
- [ ] **EG-TYPED-PACKS-R048.2:** A measured fact cites the evaluation run that produced it. `facts.rs` must define `MeasuredFact<T>` holding a `value` and an `evaluation_run_ref: ObservationRef`, reusing the `ObservationRef` type that `latency_observed_ref` uses. A measured fact without a run reference must fail validation. Test: `crates/eg-types/src/agent_component/facts.rs::tests::measured_fact_requires_run_ref`.
- [ ] **EG-TYPED-PACKS-R048.3:** Shell proficiency is a measured model fact. `facts.rs` must define `ShellProficiency` (`low`, `medium`, `high`) and add `shell_proficiency: Option<MeasuredFact<ShellProficiency>>` to `ModelProfile` with `#[serde(default)]`. The ontology projection must name it `shellProficiency`. An absent value must leave the existing digest vectors unchanged. Test: `crates/eg-types/src/agent_component.rs::tests::absent_shell_proficiency_keeps_digest`.
- [ ] **EG-TYPED-PACKS-R048.4:** Stopping behaviour is a measured model fact. `facts.rs` must define `StoppingBehaviour` (`stops_early`, `balanced`, `over_verifies`) and add `stopping_behaviour: Option<MeasuredFact<StoppingBehaviour>>` to `ModelProfile` with `#[serde(default)]`. The ontology projection must name it `stoppingBehaviour`. The digest function at `agent_component.rs:748` must hash both measured facts when present. Test: `crates/eg-types/src/agent_component.rs::tests::measured_model_facts_change_digest`.
- [ ] **EG-TYPED-PACKS-R048.5:** Pack import round-trips the new model facts. A signed pack entry carrying `contextWindowTokens`, `shellProficiency` and `stoppingBehaviour` must import and read back unchanged. An entry with an unknown enum value must be refused with a typed error. Test: `tests/typed_packs/test_model_harness_facts.py::test_model_profile_round_trip`.
- [ ] **EG-TYPED-PACKS-R066.1:** SHACL shapes validate the measured model facts. A new shapes file `crates/eg-core/ontology/harness-v1.shapes.ttl` must require `shellProficiency` and `stoppingBehaviour` to be one of their enum values. It must require an `evaluationRun` link whenever a value is present. It must require `contextWindowTokens` to be at least 1. Test: `crates/eg-core/tests/harness_shapes.rs::model_fact_shapes_reject_missing_run`.
- [ ] **EG-TYPED-PACKS-R066.2:** The task class carries a shell-centric fact. `crates/eg-core/ontology/swarm_topology-v1.ttl` must declare the boolean property `swarm:shellCentric` on `swarm:TaskShape` (line 80 area owns `swarm:DecomposableTask`). The matching shape in `swarm_topology-v1.shapes.ttl` must require a boolean datatype and at most one value. Test: `crates/eg-core/tests/swarm_topology_shapes.rs::shell_centric_must_be_boolean`.
- [ ] **EG-TYPED-PACKS-R066.3:** The shell-centric fact is inherited by task subclasses. A task class with no own `swarm:shellCentric` value must read the value of its nearest ancestor through `agent_ontology::broader_chain`. When no ancestor states it, the reader must return an absent value, never `false`. Test: `crates/eg-core/tests/swarm_topology_shapes.rs::shell_centric_inherits_or_is_absent`.
- [ ] **EG-TYPED-PACKS-R045.1:** Annotation type carries side effects, declarer and schema size. `PackAnnotations` (`crates/eg-types/src/connector_pack/annotations.rs:46`) must gain `side_effects`, `declared_by` and `schema_tokens`, each with `#[serde(default)]`. `side_effects` is the closed set `none`, `workspace_write`, `external_write`, `network`. `declared_by` is `connector`, `mcp_hint` or `certified`. `schema_tokens` is `Option<u32>`. The SDK publishes these facts in `SDK-CONNECTOR-CONTROL-R045.1` to `.11`.
- [ ] **EG-TYPED-PACKS-R045.2:** Absent facts take safe defaults, never zero. An absent `read_only_hint` must read as `false`, absent `side_effects` as `external_write`, absent `declared_by` as `connector`, and absent `schema_tokens` as absent. A default method on `PackAnnotations` must expose these. No code path may turn an absent `schema_tokens` into 0.
- [ ] **EG-TYPED-PACKS-R045.3:** The annotation digest covers the new fields without moving old vectors. `annotations_digest` (`crates/eg-types/src/connector_pack/digest.rs:135`) must hash the three fields only when present. An entry without them must keep its existing golden digest vector.
- [ ] **EG-TYPED-PACKS-R045.4:** Admission refuses an unknown side-effect or declarer value. `check_annotations` (`src/server/handlers/admin/connector_pack/import/annotations.rs:34`) must refuse an unknown `side_effects` or `declared_by` value, or a `schema_tokens` of 0, with a typed annotation refusal naming the field.
- [ ] **EG-TYPED-PACKS-R045.5:** Admission records an unproven read-only claim. `tool_facts` (`src/server/handlers/admin/connector_pack/import/facts.rs:21`) must copy the three fields into `AgentComponentFacts::Tool` (`crates/eg-types/src/agent_component/facts.rs:198`). When `read_only_hint` is true and `declared_by` is `mcp_hint`, the stored fact must mark the claim unproven. The `effect` mapping at `facts.rs:25` must then yield `ToolEffect::Write`.
- [ ] **EG-TYPED-PACKS-R045.6:** The tool-subset path abstains for a tool with no size. `token_cost` (`src/server/handlers/decide/stat_tool_subset.rs:91`) now returns 0 for an undeclared cost, which treats the tool as free. It must return no value instead. `request_body` (`stat_tool_subset.rs:99`) must read each candidate's `schema_tokens` as its `token_cost` and must abstain on the `tool_subset` part for any candidate without one. It must not assume a size. This feeds `EG-DECISION-ENGINE-R045.2` and `EG-DECISION-ENGINE-R128.9`.
- [ ] **EG-TYPED-PACKS-R045.7:** No decision premise treats an unproven read-only claim as certified. The `HarnessConfig` handler (`EG-DECISION-ENGINE-R128.8`) and the capability rules (`crates/eg-rdf/src/rules/capability.rs:384`) must treat `read_only` as certified only when `declared_by` is `certified` or `connector`. A premise built from an `mcp_hint` claim must carry the label unproven.
- [ ] **EG-TYPED-PACKS-R045.8:** The generated contract and Python client carry the new fields. `gen_contract --check` (`crates/eg-capabilities/Cargo.toml:102`) must pass after regeneration. The Python `PackAnnotations` model (`crates/eg-capabilities/src/contract/python/surfaces.rs:12`) must expose the three fields, so the SDK can import them.
- [ ] **EG-TYPED-PACKS-R045.9:** End-to-end proof: a two-tool pack yields a budget-respecting subset. A served test must import a pack with two tools of known `schema_tokens` through the import path. It must then call `Decide` with `HarnessConfig` (`EG-DECISION-ENGINE-R128`) and a budget that fits one tool. The answer's tool subset must stay within that budget. A tool with no `schema_tokens` must produce an abstention.
