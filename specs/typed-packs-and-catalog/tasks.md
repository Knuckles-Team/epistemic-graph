# Work breakdown and traceability

The state of every task in this file is `VERIFY` until exact merged-head evidence is recorded. `VERIFY` means code may already exist, but the contract and acceptance are still to be checked. `BUILD` means a known unmet behavior is specified here. `DEFERRED` is a separately bounded capability. `REJECTED` records a prohibited path. Task checkboxes track work performed; they never turn an untested implementation into `ACCEPTED`.

## Pack contract and admission

- [ ] **P1 — VERIFY:** Freeze the typed pack index, entry, annotation, digest, method/result and golden-vector contract; prove all generated clients share the same bytes and result types (PACK-01/02/10; EH-075, EH-079, EH-093, EH-135, EH-136, EH-198, EH-199).
- [ ] **P2 — VERIFY:** Prove engine-owned body staging, guarded holder/refcount, orphan reconciliation, exact body read and content model across unchanged, revised, withdrawn and retired members (PACK-03/04; EH-077, EH-083, EH-089, EH-091, EH-127).
- [ ] **P3 — VERIFY:** Prove Import boundary, importer binding, one atomic owner commit, replay, audit, preflight, cancellation and tenant-scoped lock release; keep methods LocalOnly in clustered mode (PACK-03/11; EH-078, EH-082, EH-085, EH-087, EH-090, EH-095, EH-096, EH-124, EH-536).
- [ ] **P4 — VERIFY:** Run planted mutation, security, tenancy, crash, SDK and end-to-end connector fixtures. Fix G14 timeout and G15 malformed-path admission if still reproducible (PACK-06/12; EH-080, EH-587).
- [ ] **P5 — VERIFY:** Execute G1–G21 from `architecture.md` including negative state invariants (PACK-06; EH-106, EH-107, EH-108, EH-109, EH-110, EH-112, EH-113, EH-114, EH-115, EH-116, EH-117, EH-118, EH-120, EH-121, EH-122, EH-123, EH-125, EH-126). Consume the shared EH-119 consistency result from `federated-query-and-reasoning`.
- [ ] **P6 — VERIFY:** Keep capability, modality, schema digest, MCP hints, cost, latency and model facts as typed annotations; classify native versus foreign claims without inventing a native fact (PACK-09; EH-088, EH-128, EH-129, EH-130, EH-131, EH-132, EH-133, EH-134, EH-200).

## Projection, schema and catalog

- [ ] **P7 — VERIFY:** Run the first Agent Library outbox consumer with head-state projection, current-record graph attach, visibility CAS and failure/re-drive; prove authorization of the worker's reserved graph write (PACK-07; EH-081, EH-145, EH-146, EH-148, EH-149, EH-150, EH-151, EH-153, EH-370).
- [ ] **P8 — VERIFY:** Reuse a single bounded schema validator for pack and graph source composition, ensure shape ownership and monotone pack revisions, and expose `GraphSchemaClasses` with committed digest (PACK-08; EH-137, EH-152, EH-164, EH-166, EH-167, EH-337, EH-364, EH-389). Consume EH-355/EH-363 reasoning from `federated-query-and-reasoning`.
- [ ] **P9 — VERIFY:** Keep ServerRegistry as liveness authority and ConnectorPack/AgentComponent as content authority; remove shadow catalog consumers after typed reads are available (PACK-09; EH-345). Consume EH-377 generated models from `public-engine-contract-and-release`.
- [ ] **P10 — VERIFY:** Make pack contract regeneration and artifact transfer compare content and digest across `contract/`, Python generated outputs and Rust catalog digest (PACK-10; EH-237, EH-266, EH-324, EH-440). Consume Go/JS signing and Python typed result parity from `public-engine-contract-and-release`.
- [ ] **P11 — VERIFY:** Run complete Rust integration, root Python, client and release suites, consuming the shared dispatch-decomposition, Python CI and naming gates owned by `public-engine-contract-and-release` (PACK-11/12; EH-371, EH-376, EH-591).
- [ ] **P12 — BUILD/VERIFY:** Move graph ontology object and schema DTO authority into EG typed models and generated clients; route schema-drift candidate SHACL construction and contract/activation storage through EG while the connector-sync runner retains its drain/apply gate. Remove the prior duplicate DTO and ontology owner only after a generated-type parity and migration test (PACK-08/10; EH-504, EH-506, EH-510).

## Ruled decisions and boundaries

| IDs | Decision or boundary preserved by this spec |
|---|---|
| EH-084 | Projection readiness is the atomicity boundary for readers. |
| EH-085 | Use one `Import` operation; no prepare/commit/abort family. |
| EH-086, EH-201 | Incompatible Agent Library layout is refused; offline owner-file upgrade is rejected. |
| EH-087 | Pack and four agent-layer write methods stay LocalOnly. |
| EH-088 | Foreign capability IRIs remain claims; mistyped native IRIs refuse. |
| EH-089 | Missing pack members become reversible Withdrawn. |
| EH-090 | Bind/Unbind is admin controlled; configured importer is bootstrap default. |
| EH-091 | Server pin follows declared contract and instructions, not package version. |
| EH-092 | The accepted design includes projection but excludes offline upgrade. |
| EH-093 | Body read uses `AgentComponent.Content`. |
| EH-094 | Only `SKILL.md` is imported as skill content. |
| EH-095 | The AU tiny-profile admin issue is an adjacent consumer fix, not an EG pack write. |
| EH-096 | Cross-cutting defects affecting safe import are release-gate obligations. |
| EH-145 | Projection waits for the general outbox reject/re-drive primitive. |
| EH-146 | Non-pack graph attachment is a stale-visible snapshot until a durable auto-follow design exists. |
| EH-148–EH-154 | Topic isolation, failure severity, manifest refusal, TBox composition, bounded reasoner, delta validation and owner-layout ordering are architectural corrections described in `architecture.md`. |
| EH-164, EH-166, EH-167 | Schema source capacity, GraphState replication and agent-row schema migration must be verified against current typed contracts. |

## Adjacent rows discovered in the broad catalog crosswalk

These IDs do not create pack behavior. They are retained here so a contributor cannot silently count them as accepted by this spec. Their full requirements must live in their owning feature spec before delivery; only the stated shared boundary applies here.

| IDs | Adjacent scope and pack boundary |
|---|---|
| EH-280 | Repository ingestion of branch/blob content; may use typed source facts but is not a ConnectorPack import. |
| EH-394 | Retrieval plan candidates; consumes visible typed skills and schema digest, but solver choice is outside this spec. |
| EH-413 | Typed OHLCV bars; may be represented in the shared catalog but time-series semantics are separate. |
| EH-537, EH-560 | Identity method family and governed approval; pack authorization must use the common capability system. |
| EH-635 | Embedded/served authority parity applies to pack reads when exposed; wider embedded transport expansion is separate. |
| EH-663, EH-693 | Database schema profiling/context consume committed schema facts; source inference/query design is separate. |
| EH-706 | Finance leverage model uses typed data; trading and risk semantics are separate. |
| EH-720, EH-721 | Batch pipelining and SQL plan/result cache are data-plane capabilities, not pack storage. |

## Done criteria

Mark a PACK requirement `LANDED` only with an exact merged EG commit. Mark it `ACCEPTED` only after the mapped tests in `test-spec.md`, generated contract receipt, security proof, configured quality gates and served-path verification pass on that commit. A ruled decision row is documented rather than implemented again. A queued adjacent row cannot be closed through this spec.
