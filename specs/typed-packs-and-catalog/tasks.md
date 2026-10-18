# Work breakdown and traceability

The generator owns delivery state in `status.json`.
Task checkboxes record historical work; exact source and executed scenarios establish evidence.
The new prompt scenarios have no execution receipts.

## Pack contract and admission

- [x] **P1:** Freeze the typed pack index, entry, annotation, digest, method/result and golden-vector contract. Prove all generated clients share the same bytes and result types (PACK-01/02/10; EG-TYPED-PACKS-R001, EG-TYPED-PACKS-R004, EG-TYPED-PACKS-R018, EG-TYPED-PACKS-R049, EG-TYPED-PACKS-R050, EG-TYPED-PACKS-R064, EG-TYPED-PACKS-R065).
- [x] **P2:** Prove engine-owned body staging, guarded holder/refcount, orphan reconciliation, exact body read and content model across unchanged, revised, withdrawn and retired members (PACK-03/04; EG-TYPED-PACKS-R002, EG-TYPED-PACKS-R008, EG-TYPED-PACKS-R014, EG-TYPED-PACKS-R016, EG-TYPED-PACKS-R041).
- [x] **P3:** Prove Import boundary, importer binding, one atomic owner commit, replay, audit, preflight, cancellation and tenant-scoped lock release. Keep methods LocalOnly in clustered mode (PACK-03/11; EG-TYPED-PACKS-R003, EG-TYPED-PACKS-R007, EG-TYPED-PACKS-R010, EG-TYPED-PACKS-R012, EG-TYPED-PACKS-R015, EG-TYPED-PACKS-R020, EG-TYPED-PACKS-R021, EG-TYPED-PACKS-R038, EG-TYPED-PACKS-R084).
- [x] **P4:** Run planted mutation, security, tenancy, crash, SDK and end-to-end connector fixtures. Fix G14 timeout and G15 malformed-path admission if still reproducible (PACK-06/12; EG-TYPED-PACKS-R005, EG-TYPED-PACKS-R086).
- [x] **P5:** Execute G1–G21 from `architecture.md`, including negative state invariants (PACK-06). Related IDs include EG-TYPED-PACKS-R022 through EG-TYPED-PACKS-R031. They also include EG-TYPED-PACKS-R032 through EG-TYPED-PACKS-R037, plus EG-TYPED-PACKS-R039 and EG-TYPED-PACKS-R040. Consume the shared EG-FEDERATED-QUERY-R001 consistency result from `federated-query-and-reasoning`.
- [x] **P6:** Keep capability, modality, schema digest, MCP hints, cost, latency and model facts as typed annotations. Classify native versus foreign claims without inventing a native fact (PACK-09; EG-TYPED-PACKS-R013, EG-TYPED-PACKS-R042, EG-TYPED-PACKS-R043, EG-TYPED-PACKS-R044, EG-TYPED-PACKS-R045, EG-TYPED-PACKS-R046, EG-TYPED-PACKS-R047, EG-TYPED-PACKS-R048, EG-TYPED-PACKS-R066).

## Projection, schema and catalog

- [x] **P7:** Run the first Agent Library outbox consumer with head-state projection, current-record graph attach, visibility CAS and failure/re-drive. Prove authorization of the worker's reserved graph write (PACK-07; EG-TYPED-PACKS-R006, EG-TYPED-PACKS-R052, EG-TYPED-PACKS-R053, EG-TYPED-PACKS-R054, EG-TYPED-PACKS-R055, EG-TYPED-PACKS-R056, EG-TYPED-PACKS-R057, EG-TYPED-PACKS-R059, EG-TYPED-PACKS-R074).
- [x] **P8:** Reuse a single bounded schema validator for pack and graph source composition, ensure shape ownership and monotone pack revisions, and expose `GraphSchemaClasses` with committed digest (PACK-08; EG-TYPED-PACKS-R051, EG-TYPED-PACKS-R058, EG-TYPED-PACKS-R061, EG-TYPED-PACKS-R062, EG-TYPED-PACKS-R063, EG-TYPED-PACKS-R071, EG-TYPED-PACKS-R073, EG-TYPED-PACKS-R077). Consume EG-FEDERATED-QUERY-R010/EG-FEDERATED-QUERY-R012 reasoning from `federated-query-and-reasoning`.
- [x] **P9:** Keep ServerRegistry as liveness authority and ConnectorPack/AgentComponent as content authority. Remove shadow catalog consumers after typed reads are available (PACK-09; EG-TYPED-PACKS-R072). Consume EG-CONTRACT-R005 generated models from `public-engine-contract-and-release`.
- [x] **P10:** Make pack contract regeneration and artifact transfer compare content and digest across `contract/`, Python generated outputs and Rust catalog digest (PACK-10; EG-TYPED-PACKS-R067, EG-TYPED-PACKS-R068, EG-TYPED-PACKS-R070, EG-TYPED-PACKS-R080). Consume Go/JS signing and Python typed result parity from `public-engine-contract-and-release`.
- [x] **P11:** Run complete Rust integration, root Python, client and release suites. Consume the shared dispatch-decomposition, Python CI and naming gates owned by `public-engine-contract-and-release` (PACK-11/12; EG-TYPED-PACKS-R075, EG-TYPED-PACKS-R076, EG-TYPED-PACKS-R087).
- [x] **P12:** Move graph ontology object and schema DTO authority into EG typed models and generated clients. Route schema-drift candidate SHACL construction and contract/activation storage through EG while the connector-sync runner retains its drain/apply gate. Remove the prior duplicate DTO and ontology owner only after a generated-type parity and migration test (PACK-08/10; EG-TYPED-PACKS-R081, EG-TYPED-PACKS-R082, EG-TYPED-PACKS-R083).

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
| EG-TYPED-PACKS-R054–EG-TYPED-PACKS-R060 | Topic isolation, failure severity, manifest refusal, TBox composition, bounded reasoner, delta validation and owner-layout ordering. These architectural corrections are described in `architecture.md`. |
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

- [x] **P13:** Pin the `OwnerLayout::Blob` body-holder table cardinality, regenerate ontology/SHACL artifacts byte-for-byte from the typed Rust vocabulary, import FoodData Central/FoodOn nutrient reference types through a ChEBI crosswalk, exclude the request ID from the Python client's import idempotency key, isolate Turtle blank-node identities per parse, serve `AgentComponent.content` pack-body bytes directly, surface `GraphSchema` attach staleness, and reproject a connector pack with bounded, isolated, restart-safe aggregates (EG-TYPED-PACKS-R089, EG-TYPED-PACKS-R090, EG-TYPED-PACKS-R091, EG-TYPED-PACKS-R092, EG-TYPED-PACKS-R093, EG-TYPED-PACKS-R094, EG-TYPED-PACKS-R095, EG-TYPED-PACKS-R096). EG-TYPED-PACKS-R092 is proven directly by `tests/test_connector_pack_client.py::test_import_key_excludes_the_request_id`, which varies only `request_id` across two imports and asserts the operation key does not move; the remaining seven requirements in this line are still open.

Requirement IDs in `requirements.md` not mentioned by any task above before this line: none remain — EG-TYPED-PACKS-R089 through EG-TYPED-PACKS-R096 are closed by P13.

## Done criteria

Record exact merged source, mapped test results, generated contract receipts, security proof and served-path evidence.
Keep each ruled boundary and adjacent owner obligation intact.
- [x] **P14 (EG-TYPED-PACKS-R097):** Publish the human-resources, legal, medical and government vocabularies as typed domain packs. Include validation, import and round-trip tests. Then confirm callers read the packs.
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
- [x] **EG-TYPED-PACKS-R048.3:** Shell proficiency is a measured model fact. `facts.rs` must define `ShellProficiency` (`low`, `medium`, `high`) and add `shell_proficiency: Option<MeasuredFact<ShellProficiency>>` to `ModelProfile` with `#[serde(default)]`. The ontology projection must name it `shellProficiency`. An absent value must leave the existing digest vectors unchanged. Test: `crates/eg-types/src/agent_component.rs::tests::absent_shell_proficiency_keeps_digest`.
- [x] **EG-TYPED-PACKS-R048.4:** Stopping behaviour is a measured model fact. `facts.rs` must define `StoppingBehaviour` (`stops_early`, `balanced`, `over_verifies`) and add `stopping_behaviour: Option<MeasuredFact<StoppingBehaviour>>` to `ModelProfile` with `#[serde(default)]`. The ontology projection must name it `stoppingBehaviour`. The digest function at `agent_component.rs:748` must hash both measured facts when present. Test: `crates/eg-types/src/agent_component.rs::tests::measured_model_facts_change_digest`.
- [x] **EG-TYPED-PACKS-R048.5:** Pack import round-trips the new model facts. A signed pack entry carrying `contextWindowTokens`, `shellProficiency` and `stoppingBehaviour` must import and read back unchanged. An entry with an unknown enum value must be refused with a typed error. Test: `tests/typed_packs/test_model_harness_facts.py::test_model_profile_round_trip`.
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
- [ ] EG-TYPED-PACKS-R100.1 Add the rendered skill package type
- [ ] EG-TYPED-PACKS-R100.2 Render a package from graph content
- [ ] EG-TYPED-PACKS-R100.3 Render when the catalog row is read
- [ ] EG-TYPED-PACKS-R100.4 Refuse a skill with a dangling reference
- [ ] EG-TYPED-PACKS-R100.5 Record the revision of every render
- [ ] EG-TYPED-PACKS-R100.6 Prove regeneration after a change

## Open-vocabulary crosswalk definitions

- [ ] **EG-TYPED-PACKS-R071.1:** **Versioned core vocabulary crosswalks (rollup).** Extend R071 authority with explicit Schema.org mappings for person, event and document metadata. RDF/OWL semantics and upper-ontology disjointness remain authoritative in EG. Children `.1` through `.6` define this contract.
- [ ] **EG-TYPED-PACKS-R071.1.1:** **Model.** Add `VocabularyCrosswalk` beside `DomainClass` in `crates/eg-types/src/domain_pack.rs`. Carry source and target absolute class IRIs, relation, source revision, target vocabulary revision, origin and content digest. Relations are closed: `SeeAlso`, `SkosExactMatch`, `SkosCloseMatch`, `SkosBroadMatch`, `SkosNarrowMatch`. Store approved target declarations and their categories with the vocabulary revision.
- [ ] **EG-TYPED-PACKS-R071.1.2:** **Behavior.** Use the exact source/target/relation fixtures in the test specification for EG Person, Event and Document. Their declared relation is `SeeAlso`; Document includes physical documents and must not become `SkosExactMatch` to DigitalDocument. `SkosBroadMatch` means the target is broader than the source. `SkosNarrowMatch` means the target is narrower. Choose each relation explicitly in typed authority. References never imply `owl:sameAs`, class equivalence or new subclass assertions. Preserve continuant, process and temporal-region separation. Use named limits: 4096 source classes, 16384 mappings and 2048 bytes per IRI.
- [ ] **EG-TYPED-PACKS-R071.1.3:** **Wiring.** Extend `VocabularyModule::to_turtle` at `crates/eg-core/src/ontology_codegen.rs:45` to emit declared crosswalks. Reuse schema composition in `src/server/graph_schema.rs::apply_pack_source` before activation. Extend `OntologyInspection` and `src/server/handlers/rdf/inspect.rs::handle_ontology_inspect` to return crosswalks from composed sources. The production caller is `Method::OntologyInspect` in `src/server/handlers/rdf/dispatch.rs:314`. These crosswalk call sites remain to be implemented.
- [ ] **EG-TYPED-PACKS-R071.1.4:** **Refusal.** Resolve every source IRI as a class in the pinned source revision and selected source scope before activation. Return `CROSSWALK_SOURCE_UNKNOWN` for an unresolved class, including a property IRI. Return `CROSSWALK_SOURCE_OUT_OF_SCOPE` for a known class outside the authorized selected source set. Include the source IRI, requested revision and scope. Reject unresolved targets, conflicting relations and mappings incompatible with approved target categories before activation. Return `CROSSWALK_TARGET_UNKNOWN`, `CROSSWALK_CONFLICT`, `CROSSWALK_CATEGORY_CONFLICT` or `CROSSWALK_LIMIT_EXCEEDED`, with the offending IRI. Existing schema consistency and disjointness checks still run. Never fetch or silently adopt external definitions during import.
- [ ] **EG-TYPED-PACKS-R071.1.5:** **Evidence.** Return source and target IRIs, relation, both vocabulary revisions, origin, crosswalk digest and committed schema digest. Distinguish inline inspection from committed-source inspection. Bind report evidence to the inspected tenant, graph and source set.
- [ ] **EG-TYPED-PACKS-R071.1.6:** **Hot-path proof.** Entry point: authenticated `ConnectorPack.Import`, followed by schema attachment and served `OntologyInspect` over composed sources. `tests/test_ontology_crosswalk_served.py::test_core_metadata_crosswalks` imports person, event and document fixtures using the real client and server. Query committed classes through `GraphSchemaClasses` and inspect mappings separately.
- [ ] **EG-TYPED-PACKS-R097.2:** **Domain crosswalks and exact unmapped-class reports (rollup).** Extend R097 with medical, technology and labor crosswalks to SKOS and Schema.org. Reuse R071.1 records and keep ontology authority inside EG. Children `.1` through `.6` define this contract.
- [ ] **EG-TYPED-PACKS-R097.2.1:** **Model.** Extend `DomainPack` with versioned crosswalks and `CrosswalkCoverage{declared,mapped,unmapped,source_digest}`. Each unmapped item carries its class IRI and `NoDeclaredMapping` reason. Add `Technology` to `DomainPackKind`; labor uses existing `Hr`, and medical uses existing `Medical`. Preserve the existing HR model and its validation.
- [ ] **EG-TYPED-PACKS-R097.2.2:** **Behavior.** Compute coverage over every class declared by the selected pack revision. A class is mapped only when it has a validated declared crosswalk. Return the exact sorted set difference as unmapped classes. Missing mappings never infer equivalence or import external authority. Reuse R071.1 bounds and typed vocabulary generation.
- [ ] **EG-TYPED-PACKS-R097.2.3:** **Wiring.** Extend `validate_and_commit` in `src/server/handlers/admin/connector_pack/import.rs:45` before publishing a pack head. Its production caller is `serve_import` in `src/server/handlers/admin/connector_pack.rs:132`. Validate crosswalks again at `src/server/graph_schema.rs::apply_pack_source` before activating schema. Extend the existing served `OntologyInspect` result with revision-bound coverage. The domain-to-import crosswalk path does not exist today; this row creates the call site.
- [ ] **EG-TYPED-PACKS-R097.2.4:** **Refusal.** Missing mappings produce explicit unmapped results and never count as full coverage. Invalid mappings use R071.1 typed refusals and block pack publication or schema activation. Source resolution uses the selected pack revision, not the global class catalog. Unknown or out-of-scope sources never become unmapped entries. Reject stale vocabulary revisions and caller-forged origin or coverage with `CROSSWALK_REVISION_MISMATCH` or `CROSSWALK_EVIDENCE_INVALID`.
- [ ] **EG-TYPED-PACKS-R097.2.5:** **Evidence.** Bind coverage to tenant, graph, pack digest, pack revision, target vocabulary revision and committed schema digest. Return exact mapped and unmapped class sets with reasons and declared totals. Source connectors only supply candidate data; EG validates and records authority.
- [ ] **EG-TYPED-PACKS-R097.2.6:** **Hot-path proof.** Entry point: authenticated `ConnectorPack.Import`, schema attachment, `OntologyInspect` and `GraphSchemaClasses` through the generated client. `tests/test_ontology_crosswalk_served.py::test_domain_coverage_and_refusals` loads medical, technology and labor fixtures. It exercises unmapped results, unknown and out-of-scope source classes, conflicting mappings, stale revisions, category violations and restart readback.


## Atomic prompt and original acceptance units

- [ ] **EG-TYPED-PACKS-R001.1:** Original pack index and entry types. Execute the positive and negative proving cases in test-spec.md.
- [ ] **EG-TYPED-PACKS-R001.2:** Original section and annotation types. Execute the positive and negative proving cases in test-spec.md.
- [ ] **EG-TYPED-PACKS-R001.3:** Original pack and content operations. Execute the positive and negative proving cases in test-spec.md.
- [ ] **EG-TYPED-PACKS-R001.4:** Original framed digest vectors. Execute the positive and negative proving cases in test-spec.md.
- [ ] **EG-TYPED-PACKS-R001.5:** All generated clients share the pack contract. Execute the positive and negative proving cases in test-spec.md.
- [ ] **EG-TYPED-PACKS-R001.6:** Versioned prompt graph identity. Execute the positive and negative proving cases in test-spec.md.
- [ ] **EG-TYPED-PACKS-R001.7:** Prompt payload validation. Execute the positive and negative proving cases in test-spec.md.
- [ ] **EG-TYPED-PACKS-R001.8:** Deterministic runtime rendering. Execute the positive and negative proving cases in test-spec.md.
- [ ] **EG-TYPED-PACKS-R001.9:** Generated prompt operation contract. Execute the positive and negative proving cases in test-spec.md.
- [ ] **EG-TYPED-PACKS-R002.1:** Original engine body staging. Execute the positive and negative proving cases in test-spec.md.
- [ ] **EG-TYPED-PACKS-R002.2:** Original unchanged holder transition. Execute the positive and negative proving cases in test-spec.md.
- [ ] **EG-TYPED-PACKS-R002.3:** Original revised holder transition. Execute the positive and negative proving cases in test-spec.md.
- [ ] **EG-TYPED-PACKS-R002.4:** Original withdrawn holder transition. Execute the positive and negative proving cases in test-spec.md.
- [ ] **EG-TYPED-PACKS-R002.5:** Original orphan grace and fault recovery. Execute the positive and negative proving cases in test-spec.md.
- [ ] **EG-TYPED-PACKS-R002.6:** Original verified body serving. Execute the positive and negative proving cases in test-spec.md.
- [ ] **EG-TYPED-PACKS-R002.7:** Native prompt body persistence. Execute the positive and negative proving cases in test-spec.md.
- [ ] **EG-TYPED-PACKS-R002.8:** Prompt graph projection. Execute the positive and negative proving cases in test-spec.md.
- [ ] **EG-TYPED-PACKS-R002.9:** Served prompt rendering. Execute the positive and negative proving cases in test-spec.md.
- [ ] **EG-TYPED-PACKS-R002.10:** Refusal before model dispatch. Execute the positive and negative proving cases in test-spec.md. Name the AU owning requirement and actual dispatch call site; source-effect producers do not satisfy this task.
- [ ] **EG-TYPED-PACKS-R002.11:** Durable prompt restart proof. Execute the positive and negative proving cases in test-spec.md.
- [ ] **EG-TYPED-PACKS-R094.1:** Original Stable content route. Execute the positive and negative proving cases in test-spec.md.
- [ ] **EG-TYPED-PACKS-R094.2:** Pinned native blueprint export. Execute the positive and negative proving cases in test-spec.md.
- [ ] **EG-TYPED-PACKS-R094.3:** Modified native blueprint import. Execute the positive and negative proving cases in test-spec.md.
- [ ] **EG-TYPED-PACKS-R094.4:** Blueprint retry identity. Execute the positive and negative proving cases in test-spec.md.
- [ ] **EG-TYPED-PACKS-R094.5:** Served lossless round trip. Execute the positive and negative proving cases in test-spec.md.
- [ ] **EG-TYPED-PACKS-R094.6:** Optimization proposals carry no authority. Execute the positive and negative proving cases in test-spec.md.
- [ ] **EG-TYPED-PACKS-R094.7:** Approved repository-source effect dependency. Execute the positive and negative proving cases in test-spec.md. Consume the pinned SDK R004.5–R004.12 and GraphOS R003.4–R003.10 contracts through their real served composition.

<!-- spec-definition-patch:owner-batch-20261012-typed-packs-and-catalog -->
## Owner definition tasks — 2026-10-12

- [ ] **`EG-TYPED-PACKS-R100.7`:** Bind exact rendered package identity to a recorded-run receipt at the existing `skill_row` publication boundary; refuse missing/mismatched/failing evidence. Hold positive AU acceptance until AU defines the receipt producer, run selection and pass rubric.
- [ ] **`EG-TYPED-PACKS-R097.2.7`:** Add external-to-platform mappings for the closed term-kind set. Add atomic precommit/activation validation and revision-bound all-term `Mapped`/`Unmapped{NoDeclaredMapping}` coverage on existing ontology reads. Preserve partial unmapped reporting and R071.1 direction.
- [ ] **`EG-TYPED-PACKS-R100.8`:** Add canonical workflow RDF export/import with full dataset preservation and digest/revision binding. Hold the strict-newer baseline/order decision and consumer wiring. Resolve them after identifying the stored workflow version record and import owner operation.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:reviewed-mapped-subsets-20261012 -->
## Mapped definition tasks — 2026-10-12

- [ ] `EG-TYPED-PACKS-R069.100`: Define the typed request/result and policy/revision binding. Specify producer/consumer wiring, refusal cases and receipt/readback fields from requirements. Preserve `R069` and its prior children.
- [ ] `EG-TYPED-PACKS-R069.100` acceptance: Prove exact full-identity/profile replay is idempotent. Prove the same blob under a new branch/path writes a new source binding and provenance. Prove cosmetic content changes update source provenance without semantic graph mutation. Prove profile revision forces reparse and recomputation. Prove structural change mutates semantic graph. In every case, read back the source-binding revision separately from the semantic graph revision. Raw digest alone never authorizes a full no-op. Planned proving tests (names only; not present or executed evidence): `tests/typed_packs/test_repository_fingerprint_identity_served.py::test_source_identity_profile_drives_mutation_decision`, `tests/typed_packs/test_repository_fingerprint_identity_served.py::test_unsupported_language_uses_raw_digest_only`.
- [ ] `EG-TYPED-PACKS-R100.100`: Define the typed request/result and policy/revision binding. Specify producer/consumer wiring, refusal cases and receipt/readback fields from requirements. Preserve `R100` and its prior children.
- [ ] `EG-TYPED-PACKS-R100.100`: add served acceptance for export/edit/reimport. Change one frontmatter value and one step. Assert identical bytes for all non-target ranges, custom fields, comments, spacing, ordering and CRLF/LF style. Malformed anchors, overlapping patches, stale source digest and unknown edit spans refuse without a write. A prompt-blueprint JSON roundtrip is not accepted as evidence. Planned proving tests (names only; not present or executed evidence): `tests/typed_packs/test_skill_lossless_edit_served.py::test_frontmatter_and_step_edits_preserve_unmodified_bytes`, `tests/typed_packs/test_skill_lossless_edit_served.py::test_stale_or_ambiguous_edit_refuses_before_write`.
- [ ] `EG-TYPED-PACKS-R100.101`: Define the typed request/result and policy/revision binding. Specify producer/consumer wiring, refusal cases and receipt/readback fields from requirements. Preserve `R100` and its prior children.
- [ ] `EG-TYPED-PACKS-R100.101`: add served acceptance for these cases: serve a compatible upgrade; refuse a breaking upgrade without migration before effects; and serve a breaking upgrade with exact declared migration. A changed consumer contract, package digest or migration receipt invalidates admission. Keep the historical package readable. Read back the admitted new version only after validation. Planned proving tests (names only; not present or executed evidence): `tests/typed_packs/test_skill_compatibility_admission_served.py::test_breaking_upgrade_requires_exact_consumer_migration`, `tests/typed_packs/test_skill_compatibility_admission_served.py::test_changed_consumer_contract_invalidates_admission`.
- [ ] `EG-TYPED-PACKS-R001.100`: Define the typed request/result and policy/revision binding. Specify producer/consumer wiring, refusal cases and receipt/readback fields from requirements. Preserve `R001` and its prior children.
- [ ] `EG-TYPED-PACKS-R001.100`: add served acceptance for nested schemas and version metadata. Round-trip them through generated clients and canonical digest vectors. Test additive optional-field evolution, required-field removal, incompatible type change, stale base digest and unknown nested fields. Verify SDK authority remains separate. Prove pack admission causes no tool dispatch. Planned proving tests (names only; not present or executed evidence): `tests/typed_packs/test_connector_nested_schema_served.py::test_nested_schema_evolution_preserves_digest`, `tests/typed_packs/test_connector_nested_schema_served.py::test_incompatible_nested_schema_refuses_before_dispatch`.
- [ ] `EG-TYPED-PACKS-R072.100`: Define the typed request/result and policy/revision binding. Specify producer/consumer wiring, refusal cases and receipt/readback fields from requirements. Preserve `R072` and its prior children.
- [ ] `EG-TYPED-PACKS-R072.100`: add served acceptance for registry tests. Register and read a workflow. Prove no eager AU engine initialization and one initialization on first invocation. Invoke by registry id. Refuse unknown/withdrawn/stale ids with no run. Read back registry and invocation receipt independently. Planned proving tests (names only; not present or executed evidence): `tests/typed_packs/test_workflow_registry_served.py::test_registration_is_lazy_and_invokes_exact_revision`, `tests/typed_packs/test_workflow_registry_served.py::test_unknown_withdrawn_or_stale_registry_id_refuses`. Owner prerequisite: AU-CONTROL-R037.5 fills only a requirement gap. It does not define lazy initialization timing, the first-use operation or the execution receipt. No exact AU companion requirement ID is identified. Keep that consumer as an explicit hold until its owner publishes the operation and receipt. No invocation evidence is claimed.
- [ ] `EG-TYPED-PACKS-R071.100`: add the typed request/result, policy/revision binding, producer/consumer wiring contract, refusal cases and receipt/readback fields described in requirements; preserve `R071` and its prior children.
- [ ] `EG-TYPED-PACKS-R071.100`: add served acceptance for: Cross-repository fixtures admit a new unique path and refuse duplicate canonical path, alias collision, incompatible parent assignment and stale vocabulary revision. Read back canonical path and unchanged published IRIs; retain R071 generated ontology authority and R001.4 digest vectors. Planned proving tests (names only; not present or executed evidence): `tests/typed_packs/test_concept_path_admission_served.py::test_owner_approved_path_grammar_is_shared`, `tests/typed_packs/test_concept_path_admission_served.py::test_duplicate_or_conflicting_paths_refuse_without_iri_changes`. Owner decision hold: the canonical segment grammar is not approved for separators/escaping, Unicode normalization, case, or whitespace. Keep admission SPECIFIED until the vocabulary owner publishes one versioned grammar shared by producers; preserve existing published IRIs.
<!-- /spec-definition-patch -->

## Connector packs as native RDF (R101, R102)

- [ ] **EG-TYPED-PACKS-R101:** Ingest contracts are native OWL/RDF relationships in the pack graph. Pass the proving test in test-spec.md.
- [ ] **EG-TYPED-PACKS-R101.1:** Model. Pass the proving test in test-spec.md.
- [ ] **EG-TYPED-PACKS-R101.2:** Model. Pass the proving test in test-spec.md.
- [ ] **EG-TYPED-PACKS-R101.3:** Behavior. Pass the proving test in test-spec.md.
- [ ] **EG-TYPED-PACKS-R101.4:** Behavior. Pass the proving test in test-spec.md.
- [ ] **EG-TYPED-PACKS-R101.5:** Wiring. Pass the proving test in test-spec.md.
- [ ] **EG-TYPED-PACKS-R101.6:** Refusal. Pass the proving test in test-spec.md.
- [ ] **EG-TYPED-PACKS-R101.7:** Evidence. Pass the proving test in test-spec.md.
- [ ] **EG-TYPED-PACKS-R101.8:** Hot-path proof. Pass the proving test in test-spec.md.
- [x] **EG-TYPED-PACKS-R101.9:** One tenant key for pack heads. Resolve SourceIngest mappings under the import tenant key.
- [ ] **EG-TYPED-PACKS-R102:** Connector surface and domain relationships are native graph relationships. Pass the proving test in test-spec.md.
- [ ] **EG-TYPED-PACKS-R102.1:** Model. Pass the proving test in test-spec.md.
- [ ] **EG-TYPED-PACKS-R102.2:** Behavior. Pass the proving test in test-spec.md.
- [ ] **EG-TYPED-PACKS-R102.3:** Behavior. Pass the proving test in test-spec.md.
- [ ] **EG-TYPED-PACKS-R102.4:** Wiring. Pass the proving test in test-spec.md.
- [ ] **EG-TYPED-PACKS-R102.5:** Refusal. Pass the proving test in test-spec.md.
- [ ] **EG-TYPED-PACKS-R102.6:** Evidence. Pass the proving test in test-spec.md.
- [ ] **EG-TYPED-PACKS-R102.7:** Hot-path proof. Pass the proving test in test-spec.md.
- [ ] **EG-TYPED-PACKS-R103:** Served runtime ontology import, revision and withdrawal. Pass the proving test in test-spec.md.
- [ ] **EG-TYPED-PACKS-R103.1:** Model. Pass the proving test in test-spec.md.
- [ ] **EG-TYPED-PACKS-R103.2:** Behavior. Pass the proving test in test-spec.md.
- [ ] **EG-TYPED-PACKS-R103.3:** Wiring. Pass the proving test in test-spec.md.
- [ ] **EG-TYPED-PACKS-R103.4:** Refusal. Pass the proving test in test-spec.md.
- [ ] **EG-TYPED-PACKS-R103.5:** Evidence. Pass the proving test in test-spec.md.
- [ ] **EG-TYPED-PACKS-R103.6:** Hot-path proof. Pass the proving test in test-spec.md.

- [ ] **EG-TYPED-PACKS-R104 (rollup):** Connect-time seed graph import, idempotent by content digest (rollup).
- [ ] **EG-TYPED-PACKS-R104.1 (Model):** Add the typed model.
- [ ] **EG-TYPED-PACKS-R104.2 (Behavior):** Implement the behavior.
- [ ] **EG-TYPED-PACKS-R104.3 (Wiring):** Wire the production call site.
- [ ] **EG-TYPED-PACKS-R104.4 (Refusal):** Add the typed refusals.
- [ ] **EG-TYPED-PACKS-R104.5 (Evidence):** Emit the evidence.
- [ ] **EG-TYPED-PACKS-R104.6 (Hot-path proof):** Add the served hot-path test.
