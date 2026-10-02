# Work breakdown and traceability

The state of every task in this file is `VERIFY` until exact merged-head evidence is recorded. `VERIFY` means code may already exist, but the contract and acceptance are still to be checked. `BUILD` means a known unmet behavior is specified here. `DEFERRED` is a separately bounded capability. `REJECTED` records a prohibited path. Task checkboxes track work performed; they never turn an untested implementation into `ACCEPTED`.

## Pack contract and admission

- [ ] **P1 — VERIFY:** Freeze the typed pack index, entry, annotation, digest, method/result and golden-vector contract; prove all generated clients share the same bytes and result types (PACK-01/02/10; EG-TYPED-PACKS-R001, EG-TYPED-PACKS-R004, EG-TYPED-PACKS-R018, EG-TYPED-PACKS-R049, EG-TYPED-PACKS-R050, EG-TYPED-PACKS-R064, EG-TYPED-PACKS-R065).
- [ ] **P2 — VERIFY:** Prove engine-owned body staging, guarded holder/refcount, orphan reconciliation, exact body read and content model across unchanged, revised, withdrawn and retired members (PACK-03/04; EG-TYPED-PACKS-R002, EG-TYPED-PACKS-R008, EG-TYPED-PACKS-R014, EG-TYPED-PACKS-R016, EG-TYPED-PACKS-R041).
- [ ] **P3 — VERIFY:** Prove Import boundary, importer binding, one atomic owner commit, replay, audit, preflight, cancellation and tenant-scoped lock release; keep methods LocalOnly in clustered mode (PACK-03/11; EG-TYPED-PACKS-R003, EG-TYPED-PACKS-R007, EG-TYPED-PACKS-R010, EG-TYPED-PACKS-R012, EG-TYPED-PACKS-R015, EG-TYPED-PACKS-R020, EG-TYPED-PACKS-R021, EG-TYPED-PACKS-R038, EG-TYPED-PACKS-R084).
- [ ] **P4 — VERIFY:** Run planted mutation, security, tenancy, crash, SDK and end-to-end connector fixtures. Fix G14 timeout and G15 malformed-path admission if still reproducible (PACK-06/12; EG-TYPED-PACKS-R005, EG-TYPED-PACKS-R086).
- [ ] **P5 — VERIFY:** Execute G1–G21 from `architecture.md` including negative state invariants (PACK-06; EG-TYPED-PACKS-R022, EG-TYPED-PACKS-R023, EG-TYPED-PACKS-R024, EG-TYPED-PACKS-R025, EG-TYPED-PACKS-R026, EG-TYPED-PACKS-R027, EG-TYPED-PACKS-R028, EG-TYPED-PACKS-R029, EG-TYPED-PACKS-R030, EG-TYPED-PACKS-R031, EG-TYPED-PACKS-R032, EG-TYPED-PACKS-R033, EG-TYPED-PACKS-R034, EG-TYPED-PACKS-R035, EG-TYPED-PACKS-R036, EG-TYPED-PACKS-R037, EG-TYPED-PACKS-R039, EG-TYPED-PACKS-R040). Consume the shared EG-FEDERATED-QUERY-R001 consistency result from `federated-query-and-reasoning`.
- [ ] **P6 — VERIFY:** Keep capability, modality, schema digest, MCP hints, cost, latency and model facts as typed annotations; classify native versus foreign claims without inventing a native fact (PACK-09; EG-TYPED-PACKS-R013, EG-TYPED-PACKS-R042, EG-TYPED-PACKS-R043, EG-TYPED-PACKS-R044, EG-TYPED-PACKS-R045, EG-TYPED-PACKS-R046, EG-TYPED-PACKS-R047, EG-TYPED-PACKS-R048, EG-TYPED-PACKS-R066).

## Projection, schema and catalog

- [ ] **P7 — VERIFY:** Run the first Agent Library outbox consumer with head-state projection, current-record graph attach, visibility CAS and failure/re-drive; prove authorization of the worker's reserved graph write (PACK-07; EG-TYPED-PACKS-R006, EG-TYPED-PACKS-R052, EG-TYPED-PACKS-R053, EG-TYPED-PACKS-R054, EG-TYPED-PACKS-R055, EG-TYPED-PACKS-R056, EG-TYPED-PACKS-R057, EG-TYPED-PACKS-R059, EG-TYPED-PACKS-R074).
- [ ] **P8 — VERIFY:** Reuse a single bounded schema validator for pack and graph source composition, ensure shape ownership and monotone pack revisions, and expose `GraphSchemaClasses` with committed digest (PACK-08; EG-TYPED-PACKS-R051, EG-TYPED-PACKS-R058, EG-TYPED-PACKS-R061, EG-TYPED-PACKS-R062, EG-TYPED-PACKS-R063, EG-TYPED-PACKS-R071, EG-TYPED-PACKS-R073, EG-TYPED-PACKS-R077). Consume EG-FEDERATED-QUERY-R010/EG-FEDERATED-QUERY-R012 reasoning from `federated-query-and-reasoning`.
- [ ] **P9 — VERIFY:** Keep ServerRegistry as liveness authority and ConnectorPack/AgentComponent as content authority; remove shadow catalog consumers after typed reads are available (PACK-09; EG-TYPED-PACKS-R072). Consume EG-CONTRACT-R005 generated models from `public-engine-contract-and-release`.
- [ ] **P10 — VERIFY:** Make pack contract regeneration and artifact transfer compare content and digest across `contract/`, Python generated outputs and Rust catalog digest (PACK-10; EG-TYPED-PACKS-R067, EG-TYPED-PACKS-R068, EG-TYPED-PACKS-R070, EG-TYPED-PACKS-R080). Consume Go/JS signing and Python typed result parity from `public-engine-contract-and-release`.
- [ ] **P11 — VERIFY:** Run complete Rust integration, root Python, client and release suites, consuming the shared dispatch-decomposition, Python CI and naming gates owned by `public-engine-contract-and-release` (PACK-11/12; EG-TYPED-PACKS-R075, EG-TYPED-PACKS-R076, EG-TYPED-PACKS-R087).
- [ ] **P12 — BUILD/VERIFY:** Move graph ontology object and schema DTO authority into EG typed models and generated clients; route schema-drift candidate SHACL construction and contract/activation storage through EG while the connector-sync runner retains its drain/apply gate. Remove the prior duplicate DTO and ontology owner only after a generated-type parity and migration test (PACK-08/10; EG-TYPED-PACKS-R081, EG-TYPED-PACKS-R082, EG-TYPED-PACKS-R083).

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

- [ ] **P13 — VERIFY:** Pin the `OwnerLayout::Blob` body-holder table cardinality, regenerate ontology/SHACL artifacts byte-for-byte from the typed Rust vocabulary, import FoodData Central/FoodOn nutrient reference types through a ChEBI crosswalk, exclude the request ID from the Python client's import idempotency key, isolate Turtle blank-node identities per parse, serve `AgentComponent.content` pack-body bytes directly, surface `GraphSchema` attach staleness, and reproject a connector pack with bounded, isolated, restart-safe aggregates (EG-TYPED-PACKS-R089, EG-TYPED-PACKS-R090, EG-TYPED-PACKS-R091, EG-TYPED-PACKS-R092, EG-TYPED-PACKS-R093, EG-TYPED-PACKS-R094, EG-TYPED-PACKS-R095, EG-TYPED-PACKS-R096).

Requirement IDs in `requirements.md` not mentioned by any task above before this line: none remain — EG-TYPED-PACKS-R089 through EG-TYPED-PACKS-R096 are closed by P13.

## Done criteria

Mark a PACK requirement `LANDED` only with an exact merged EG commit. Mark it `ACCEPTED` only after the mapped tests in `test-spec.md`, generated contract receipt, security proof, configured quality gates and served-path verification pass on that commit. A ruled decision row is documented rather than implemented again. A queued adjacent row cannot be closed through this spec.
- [ ] **P14 (EG-TYPED-PACKS-R097):** Publish the human-resources, legal, medical and government vocabularies as typed domain packs with validation, import and round-trip tests, then confirm callers read the packs.
- [ ] **P15 (EG-TYPED-PACKS-R098):** Serve the OntologyInspect method and inline data triples on ShaclValidate, regenerate the wire contract and every generated client, and prove both through handler, classification and Python client tests.
- [ ] **P16 (EG-TYPED-PACKS-R099):** Add the prototype-matching kernel and its bounded Python binding to the numeric extension with Rust unit tests and installed-binding parity and bounds tests.
