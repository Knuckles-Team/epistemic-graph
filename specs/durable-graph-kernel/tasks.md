# Tasks

Status key: `[ ]` queued, `[-]` in progress, `[x]` verified at cited main revision. All tasks below remain unchecked until an implementation PR supplies exact evidence.

- [ ] **K-01 (KG-01, KG-02):** Pin the current kernel revision, map every durable write entry point, write the cross-store atomicity ADR, and add all-or-none crash/retry tests (T-KG-01/02).
- [ ] **K-02 (KG-03, KG-04):** Validate and cache owner bindings at one transaction boundary, retain golden layout lineage/refusal cases, implement bounded scrub and typed status (T-KG-03/04).
- [ ] **K-03 (KG-05, KG-06):** Make head-only outbox attempts and reject/rewind semantics observable. Add cursor-based CDC/mirror replay, table mirroring, digest reconciliation and empty-default refusal (T-KG-05/06).
- [ ] **K-04 (KG-07):** Enumerate all served read/write paths, thread immutable verified scope and build cross-principal/tenant denial fixtures including cache/UDF/foreign/TSDB (T-KG-07).
- [ ] **K-05 (KG-08, KG-09):** Add durable audit reservation/outcome reconciliation and reserved two-person lease kinds. Generate any changed protocol/client contracts (T-KG-08/09).
- [ ] **K-06 (KG-10):** Capture failure traces automatically, restore full multi-group workload, prove repeated loaded failover and independent progress (T-KG-10).
- [ ] **K-07 (KG-11, KG-12):** Add explicit durability classes and loss-window metrics; build recoverable hot structures and linearizable typed primitives (T-KG-11).
- [ ] **K-08 (KG-13, KG-14):** Route point/structure operations before SQL planning, implement one-admission pipeline batches and schema/policy-safe plan cache (T-KG-12).
- [ ] **K-09 (KG-15):** Decide store engine by ADR, add hot/cold/tombstone parity and replication-out tooling, run differential/restore and published benchmark matrix (T-KG-13).
- [x] **K-10 (additional hardening):** Close the native-kernel hardening and test-reliability requirements no task above names: upload-cursor non-reuse, owner-manifest lineage, polygon boundary predicates, ring-buffer eviction identity, dispatch-ordering guard reliability, idempotent CreateGraph/DeleteGraph retry, blob GC grace period, holder-scoped BlobRef/BlobUnref, outbox topic classes, AVX2 kernel coverage, placement/consensus routing after sealing, `.redb` registry classification, mutation-durability classification across feature builds, create-only sealed-record guard, publication-waiter cancellation, shared-chunk GC, legacy-layout migration refusal, bounded test threading, idle-upload reclaim, lossless v2 control-state lift, compiled-shape memoization benchmark, signed-dispatch ack-loss recovery, published-contract/BlobRef parity, isolated trace capture, channel-group leave result and no-key scrub-cursor fixture — **EG-DURABLE-KERNEL-R044–EG-DURABLE-KERNEL-R069** (see `requirements.md` for each ID's definition).
- [ ] **K-11 (all):** Run configured CCCC, Dupehound, jscpd, KISS, rust architecture lint, clippy, focused tests and cloud CI at exact head. Record positive and negative evidence, then update delivery and acceptance state separately.
- [-] **K-12 (operator store upgrades):** Give the registered offline store upgrades one operator entry point (`EG-DURABLE-KERNEL-R070`): a single upgrade registry in the storage kernel, an `inspect`/`apply` command on the server binary that holds the engine's own directory lock, a startup refusal that names the command, the operator procedure in the generated owner-store format document, and command tests on disposable predecessor stores.
- [x] **K-13 (lazy agent-library open):** Add the missing startup test for `EG-DURABLE-KERNEL-R006`. Start the real server with the digest-verified binary. Use a directory fixture and a malformed-bytes fixture for `agent_library.redb`. Assert that an authenticated `Ping` succeeds and that an Agent Library operation fails on store open. Assert that `Ping` still succeeds afterward and that the process stays alive. Keep the `EG-DURABLE-KERNEL-R070` upgrade-required boot refusal intact. Recovered from the never-published agent claim `codex-eg-r006-startup-test-01a101be`.
- [x] **K-14 (R052 acceptance record):** Record the scoped acceptance of `EG-DURABLE-KERNEL-R052` in `status.json`. Cite merge `9871859d4e0cc01f0a5a2f1f07584da056cf424e`, PR 41, its merged-head receipt and CI run 36912559244. Change only the R052 row. Keep R052 delivery at `LANDED` and the whole-spec acceptance at `NOT_AUDITED`. Obtain an independent review of the exact diff first. Recovered from the never-branched agent claim `codex-r052-acceptance-record-01a0fe5f`.

## Decomposition children (tracked)

- [ ] **EG-DURABLE-KERNEL-R011:** Clippy and targeted tests cover the full and all-features builds
- [x] **EG-DURABLE-KERNEL-R011.1:** RequiredTestModuleCategory/RequiredTestModule typed category and validated declaration
- [x] **EG-DURABLE-KERNEL-R011.2:** Concrete required-module manifest. See the exact paths and proving tests in `requirements.md`.
- [ ] **EG-DURABLE-KERNEL-R011.3:** Pre-merge runtime execution. See the exact paths and proving tests in `requirements.md`.
- [ ] **EG-DURABLE-KERNEL-R011.4:** Exact-head CI acceptance receipts. See the exact paths and proving tests in `requirements.md`.
- [ ] **EG-DURABLE-KERNEL-R011.5:** Check required Clippy profile declarations and refusal cases. Runtime acceptance remains under R011.3 and R011.4.
- [ ] **EG-DURABLE-KERNEL-R014:** Raft cluster tests pass reliably under load
- [x] **EG-DURABLE-KERNEL-R014.1:** Typed flake-budget / retry-policy model for load-test retries
- [-] **EG-DURABLE-KERNEL-R014.2:** Bounded representative CPU load; process tests passed, pending main validation. See the exact paths and proving tests in `requirements.md`.
- [ ] **EG-DURABLE-KERNEL-R014.3:** Repeated loaded-test harness. See the exact paths and proving tests in `requirements.md`.
- [-] **EG-DURABLE-KERNEL-R014.3.1:** Repeated execution and retained results; process tests pass, main validation pending.
- [ ] **EG-DURABLE-KERNEL-R014.3.2:** Rust FlakeBudget-driven diagnostic retries.
- [ ] **EG-DURABLE-KERNEL-R014.4:** Loaded leader-rebalance acceptance. See the exact paths and proving tests in `requirements.md`.
- [ ] **EG-DURABLE-KERNEL-R014.5:** Loaded three-node placement acceptance. See the exact paths and proving tests in `requirements.md`.
- [ ] **EG-DURABLE-KERNEL-R024:** External graph and database backends become EG federation sources
- [x] **EG-DURABLE-KERNEL-R024.1:** PostgreSQL/AGE backend becomes a ForeignSourceSpec source
- [x] **EG-DURABLE-KERNEL-R024.2:** Fan-out backend becomes a MirrorTargetSpec federation mirror target
- [x] **EG-DURABLE-KERNEL-R024.2.1:** MirrorTargetSpec::FanOut typed spec, validation, and registration refusal
- [x] **EG-DURABLE-KERNEL-R024.2.2:** MirrorTargetRegistry registration and name-resolution entry point
- [ ] **EG-DURABLE-KERNEL-R024.3:** Cypher transpiler becomes a ForeignSourceSpec source
- [ ] **EG-DURABLE-KERNEL-R024.4:** Mirror target becomes a ForeignSourceSpec mirror target
- [x] **EG-DURABLE-KERNEL-R024.5:** Outbox-based mirror becomes a ForeignSourceSpec mirror target
- [x] **EG-DURABLE-KERNEL-R024.6:** Brain-guarded backend becomes a ForeignSourceSpec source
- [x] **EG-DURABLE-KERNEL-R024.7:** Trino backend becomes a ForeignSourceSpec source
- [x] **EG-DURABLE-KERNEL-R024.8:** Spark job backend becomes a ForeignSourceSpec source
- [x] **EG-DURABLE-KERNEL-R027:** Schema repair uses a reserved two-person approval lease
- [x] **EG-DURABLE-KERNEL-R027.1:** Governed-change model types
- [x] **EG-DURABLE-KERNEL-R027.2:** Governed-change dispatch wiring
- [x] **EG-DURABLE-KERNEL-R027.3:** Governed-change raft catalog registration
- [x] **EG-DURABLE-KERNEL-R032:** Native primary-key fast path bypasses the SQL planner
- [x] **EG-DURABLE-KERNEL-R032.1:** PrimaryKeyOperation and FastPathDecision typed routing model with authorization/audit-checked invariant and refusal
- [x] **EG-DURABLE-KERNEL-R033:** Hot-store engine choice is evaluated against captured workloads
- [x] **EG-DURABLE-KERNEL-R033.1:** HotStoreEngine/WorkloadProfile/BenchmarkResult typed model with throughput-based winner selection and refusal
- [x] **EG-DURABLE-KERNEL-R034:** Cross-store atomicity is proven and scoped to mixed transactions
- [x] **EG-DURABLE-KERNEL-R034.1:** TransactionScope/CommitPath typed model proving single-store traffic cannot reach the cross-store commit path
- [x] **EG-DURABLE-KERNEL-R035:** PostgreSQL wire protocol reaches operational and dump/restore parity
- [x] **EG-DURABLE-KERNEL-R035.1:** PgOperationalSurface/PgCompatibilityMatrix typed model, fail-closed, with named-missing-surface refusal
- [x] **EG-DURABLE-KERNEL-R036:** Mirror sinks reach fan-out parity and include Postgres table mirroring
- [x] **EG-DURABLE-KERNEL-R036.1:** MirrorSinkKind/MirrorCursor typed model with non-empty-position guard on construction and advance
- [x] **EG-DURABLE-KERNEL-R037:** Workload classifier routes point operations around the SQL planner
- [x] **EG-DURABLE-KERNEL-R037.1:** WorkloadClass typed classifier with refusal for an unrecognized operation kind
- [x] **EG-DURABLE-KERNEL-R037.2:** route dispatch entry point deciding DataFusion session allocation
- [x] **EG-DURABLE-KERNEL-R038:** KV namespaces declare an explicit, enforced durability class
- [x] **EG-DURABLE-KERNEL-R038.1:** DurabilityClass typed enum, declared loss-window bound, and refusal for an unrecognized class name
- [x] **EG-DURABLE-KERNEL-R038.2:** DurabilityClass write-path entry point: write_plan and refuse_if_durable_log_write
- [x] **EG-DURABLE-KERNEL-R039:** In-memory data structures rebuild from the redb durability log
- [x] **EG-DURABLE-KERNEL-R039.1:** DataStructureKind/AtomicPrimitiveKind/DurabilityDeclaration typed model with always-redb-sole-authority invariant
- [ ] **EG-DURABLE-KERNEL-R039.2:** Validate native control-lease consumption against engine-owned mutation time inside the row transaction; preserve conflict outcomes and revocation. Run the scoped native-record expiry regression before claiming acceptance.
- [x] **EG-DURABLE-KERNEL-R040:** Pipelined requests commit as a single batch
- [x] **EG-DURABLE-KERNEL-R040.1:** PipelineBatch/BatchCommitDecision typed model: one admission per batch, order-preserving replies, empty-batch refusal
- [x] **EG-DURABLE-KERNEL-R041:** SQL plan cache keyed by statement digest and schema version
- [x] **EG-DURABLE-KERNEL-R041.1:** PlanCacheKey/PlanCacheLookup typed key and pure lookup decision with stale-schema refusal
- [x] **EG-DURABLE-KERNEL-R041.2:** PlanCache::prepare entry point at the SQL prepare site
- [x] **EG-DURABLE-KERNEL-R042:** Benchmark gate defines and proves performance superiority claims
- [x] **EG-DURABLE-KERNEL-R042.1:** BenchmarkClaim typed report record with refusal for an unpublished or non-passing claim
- [ ] **EG-DURABLE-KERNEL-R071:** Typed durable task-queue model (enqueue/claim/ack) with refusal tests
- [ ] **EG-DURABLE-KERNEL-R072:** Served task-queue RPC over the durable outbox
- [ ] **EG-DURABLE-KERNEL-R073:** Generated client method for the task queue
- [ ] **EG-DURABLE-KERNEL-R074:** Served worker registry
- [ ] **EG-DURABLE-KERNEL-R075:** Reserved-worker admission decision
- [ ] **EG-DURABLE-KERNEL-R076:** Scheduler config and lane floors (rollup; waits for both children)
- [x] **EG-DURABLE-KERNEL-R076.1:** Pure authoritative SchedulerConfig, defaults, bounds and over-subscription refusal tests
- [ ] **EG-DURABLE-KERNEL-R076.2:** Served setting and admission/queue integration consuming R076.1
- [ ] **EG-DURABLE-KERNEL-R077:** Shard-writer width served by the scheduler surface
- [ ] **EG-DURABLE-KERNEL-R078:** Bitemporal stamping served method
- [ ] **EG-DURABLE-KERNEL-R079:** Served as-of filtering
- [ ] **EG-DURABLE-KERNEL-R080:** Served supersession
- [-] **EG-DURABLE-KERNEL-R080.1:** Atomic supersession refusal for missing successor endpoints; focused regression added, pending main validation
- [ ] **EG-DURABLE-KERNEL-R080.2:** Already-closed-fact refusal and served supersession integration coverage
- [ ] **EG-DURABLE-KERNEL-R081:** Generated client methods for bitemporal operations
- [ ] **EG-DURABLE-KERNEL-R082:** Analytics worker parity: association-rule mining jobs run on the engine's own durable job plane
- [ ] **EG-DURABLE-KERNEL-R082.1:** A submitted `MineAssociate` job is claimed and published by the colocated executor with no external worker
- [x] **EG-DURABLE-KERNEL-R082.2:** `MineAssociate` submission refuses a payload outside the governed schema
- [ ] **EG-DURABLE-KERNEL-R082.3:** Distinct-item cap and opaque-reference rule are decided and enforced at submit
- [ ] **EG-DURABLE-KERNEL-R082.4:** Association-rule identity is byte-identical to the old worker's
- [ ] **EG-DURABLE-KERNEL-R082.5:** The staged result has the 11-column knowledge-batch shape
- [x] **EG-DURABLE-KERNEL-R082.6:** Rules are ordered and thresholded exactly as before
- [ ] **EG-DURABLE-KERNEL-R082.7:** A running mining job observes cancellation, deadline and CPU budget
- [ ] **EG-DURABLE-KERNEL-R082.8:** Failure codes and retry follow the durable retry policy
- [ ] **EG-DURABLE-KERNEL-R082.9:** Colocated pool lease and poll interval are configurable
- [ ] **EG-DURABLE-KERNEL-R082.10:** Remote workers remain supported through the existing coordinator operations
- [ ] **EG-DURABLE-KERNEL-R082.11:** Cross-repo: graph-os hosts any remote worker process; agent-utilities does not

- [-] **EG-DURABLE-KERNEL-R074.1:** Typed capacity record and pure registry lifecycle with native positive/refusal tests; pending merged-head validation.
- [ ] **EG-DURABLE-KERNEL-R074.2:** Authenticated served registry integration, generated contracts and cross-owner/expiry dispatch tests.
- [ ] **EG-DURABLE-KERNEL-R083 (rollup):** Shared versioned KV-cache tier (rollup).
  - [ ] **EG-DURABLE-KERNEL-R083.1:** `KvBlockKey` typed model.
  - [ ] **EG-DURABLE-KERNEL-R083.2:** Version-mismatch refusal.
  - [ ] **EG-DURABLE-KERNEL-R083.3:** Budgeted eviction by importance.
  - [ ] **EG-DURABLE-KERNEL-R083.4:** Locate and wire the served KV operation.
  - [ ] **EG-DURABLE-KERNEL-R084:** Version-checked concurrent writes with a typed conflict (rollup).
  - [ ] **EG-DURABLE-KERNEL-R084.1:** `WriteConflict` and `MergeStrategy` types
  - [ ] **EG-DURABLE-KERNEL-R084.2:** Strategy resolution as a replaceable default
  - [ ] **EG-DURABLE-KERNEL-R084.3:** Attach at the mutation version expectation
  - [ ] **EG-DURABLE-KERNEL-R084.4:** Typed conflict refusal
  - [ ] **EG-DURABLE-KERNEL-R084.5:** Conflict record in the mutation trace
  - [ ] **EG-DURABLE-KERNEL-R084.6:** Two-writer proof


## R085 producer sequence

- [ ] **EG-DURABLE-KERNEL-R085.1:** Model. Implement the contract and planned acceptance in this directory.
- [ ] **EG-DURABLE-KERNEL-R085.2:** Versioned reads and listing. Implement the contract and planned acceptance in this directory.
- [ ] **EG-DURABLE-KERNEL-R085.3:** Sync decisions and compare-and-set. Implement the contract and planned acceptance in this directory.
- [ ] **EG-DURABLE-KERNEL-R085.4:** Production wiring. Implement the contract and planned acceptance in this directory.
- [ ] **EG-DURABLE-KERNEL-R085.5:** Authority and refusals. Implement the contract and planned acceptance in this directory.
- [ ] **EG-DURABLE-KERNEL-R085.6:** Provenance and synchronization evidence. Implement the contract and planned acceptance in this directory.
- [ ] **EG-DURABLE-KERNEL-R085.7:** Retention. Implement the contract and planned acceptance in this directory.
- [ ] **EG-DURABLE-KERNEL-R085.8:** Real served acceptance. Implement the contract and planned acceptance in this directory.

## R086 bounded acceptance slices

Each child is one focused five-minute definition or proof slice. Split larger runtime work before delivery.
This batch defines the slices; it runs none of their runtime tests.

- [ ] **EG-DURABLE-KERNEL-R086:** Safe eviction and durable read-through (rollup). Follow the matching planned acceptance in test-spec.md.
- [ ] **EG-DURABLE-KERNEL-R086.1:** Model. Follow the matching planned acceptance in test-spec.md.
- [ ] **EG-DURABLE-KERNEL-R086.2:** Behavior: memory-only eviction. Follow the matching planned acceptance in test-spec.md.
- [ ] **EG-DURABLE-KERNEL-R086.3:** Behavior: cache-miss read. Follow the matching planned acceptance in test-spec.md.
- [ ] **EG-DURABLE-KERNEL-R086.4:** Wiring. Follow the matching planned acceptance in test-spec.md.
- [ ] **EG-DURABLE-KERNEL-R086.5:** Scoped reads. Follow the matching planned acceptance in test-spec.md.
- [ ] **EG-DURABLE-KERNEL-R086.6:** Refusal: unsafe eviction. Follow the matching planned acceptance in test-spec.md.
- [ ] **EG-DURABLE-KERNEL-R086.7:** Refusal: unavailable reads. Follow the matching planned acceptance in test-spec.md.
- [ ] **EG-DURABLE-KERNEL-R086.8:** Restart. Follow the matching planned acceptance in test-spec.md.
- [ ] **EG-DURABLE-KERNEL-R086.9:** Evidence and hot-path proof. Follow the matching planned acceptance in test-spec.md.

<!-- spec-definition-patch:owner-batch-20261012-durable-graph-kernel -->
## Durable-kernel owner definition tasks — 2026-10-12

- [ ] **EG-DURABLE-KERNEL-R028.1:** Implement the R085-backed checkpoint snapshot with run/work-item/fence/checkpoint identity and separate short-/long-term memory references. Enforce verified scope, digest and expected-head refusal rules. Preserve `EG-DURABLE-KERNEL-R028`, `EG-IDENTITY-R006`, and existing GRAPHOS-A2A/OPS streaming requirements.
- [ ] **EG-DURABLE-KERNEL-R028.1 acceptance:** Checkpoint acceptance covers restart/readback and refusal of wrong scope or stale fence/revision. Also refuse digest mismatch, changed-body replay and unavailable storage. Test recovery from interruption between snapshot and pointer update. AU resume/GraphOS stream continuation remain blocked until transaction/recovery, resume-binding and retention decisions are defined.
- [ ] **EG-DURABLE-KERNEL-R028.1 status:** Keep SPECIFIED. No runtime or consumer receipt is claimed.

- [ ] **EG-DURABLE-KERNEL-R085.9:** Store complete canonical search state under R085 `active_context`, requiring `search_run_id`, `state_revision`, `transcript`, `evidence`, `constraints`, and `open_claims`. Preserve AU R013.13/R020.1 and EG R085.1/.4/.7.
- [ ] **EG-DURABLE-KERNEL-R085.9 acceptance:** Exact restart/context-eviction readback. Malformed, missing-field, oversize, wrong-authority, stale-graph, digest and backend refusals. Stale conflict retains candidate without head advance. AU continuation receipt/schema is an explicit dependency hold.
- [ ] **EG-DURABLE-KERNEL-R085.9 status:** Keep SPECIFIED. No implementation or served evidence is claimed.

- [ ] **EG-DURABLE-KERNEL-R085.10:** Persist authorized session notes via R085 with note ID, source run/task/revision, content digest, provenance and graph/revision binding. EG returns only authorized versions and conflict state. It performs no auto-injection or relevance selection. Preserve AU R022.3 and EG R085.2/.5/.7.
- [ ] **EG-DURABLE-KERNEL-R085.10 acceptance:** Acceptance covers durable readback, scope/revocation redaction, stale conflict retention and capacity refusal. AU must define capture trigger, kind mapping, relevance/staleness and conflict presentation before transparent reuse is accepted.
- [ ] **EG-DURABLE-KERNEL-R085.10 status:** Keep SPECIFIED. No automatic capture/reuse behavior is claimed.

- [ ] **EG-DURABLE-KERNEL-R084.7:** Hold cross-backend coordination until owners decide allowed writable targets, registry/coordinator owner and ordering key. They must also define atomic-versus-eventual semantics and failure/retry/idempotency/receipt behavior. Preserve one-graph R084.1-.6 and R039 as sole durability authority. Never infer writes to R024.1 sources from mirror cursors.
- [ ] **EG-DURABLE-KERNEL-R084.7 acceptance:** Existing one-graph two-writer test only. Define multi-target tests after the owner decisions. This row is not closed by R084.6.
- [ ] **EG-DURABLE-KERNEL-R084.7 status:** Keep SPECIFIED and explicitly held.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:final-mappings-20261012 -->
## Final mapped definition tasks — 2026-10-12

- [ ] `EG-DURABLE-KERNEL-R038.100`: implement the typed fields, decision/refusal behavior and owner boundary stated in requirements. Preserve `EG-DURABLE-KERNEL-R038`.
- [ ] `EG-DURABLE-KERNEL-R038.100` acceptance: Start a fresh embedded profile without an external service and read/write an explicitly ephemeral namespace. Verify no persistent namespace is silently downgraded. Start served mode against its configured endpoint and verify its declared sync/async class and acknowledgment boundary. Refuse an unknown profile and an ephemeral-to-durable class mismatch. Read back profile and namespace declarations.
<!-- /spec-definition-patch -->


<!-- spec-definition-patch:owner-dispatch-20261012-tasks -->
## Cross-owner graph feed tasks — 2026-10-12

- [ ] `EG-DURABLE-KERNEL-R043.100`: define `OpenGraphSnapshotV1` and `ReadGraphChangesV1` on committed ChangeEnvelope records with a same-read high-water cursor, ordered replay ids and bounded retention. Include tombstone filtering and explicit expired-cursor/revocation outcomes. Verify independent durable readback and EG latency through first authorized feed read. Keep GraphOS/WebUI browser commit-to-visible p95 as separate companion acceptance. Also set numeric layout limits for visible node/edge budgets, interactions and labels.
- [ ] Add typed cursor-expiry, graph-incarnation and authorization-change outcomes. Expose versioned commit-to-visible timing fields.
- [ ] Preserve R043's single MutationBatch write authority and prove two-client served delivery, reconnect ordering, independent readback and the ≤1000 ms p95 target.
<!-- /spec-definition-patch -->

## Benchmark measurement tasks — 2026-10-12

- [ ] `EG-DURABLE-KERNEL-R042.100`: extend the existing typed `BenchmarkClaim.measured_result` with complete, revision-bound `BenchmarkMeasurementV1` fields and deterministic TPS/cost formulas. Preserve existing superiority gate.
- [ ] Serve and replay exact arithmetic across releases. Exclude startup. Refuse missing token/price data, invalid elapsed time/currency and incomparable profiles.

## Spec-audit closure tasks — 2026-10-15

- [ ] **EG-DURABLE-KERNEL-R084.8:** Define tenant-scoped ordering and backend atomicity semantics. Extend conflict resolution paths and add integration tests covering two tenants, multiple backends, concurrent writes and failure/replay.
- [ ] **EG-DURABLE-KERNEL-R057.1:** Model order-snapshot lifecycle with immutable-history and derivation-link binding. Integrate with sealed-record guard. Test snapshot creation, atomic promotion, history preservation and veto refusal.
- [ ] **EG-DURABLE-KERNEL-R087.1:** Define typed changeset-review schema with five flaw classes, impact policy and finding-to-code evidence in `crates/eg-types/src/changeset_review.rs`.
- [ ] **EG-DURABLE-KERNEL-R087.2:** Serve `ChangesetReview` RPC endpoint. Integrate with GraphOS producer and AU consumer. Regenerate contracts.
- [ ] **EG-DURABLE-KERNEL-R088.1:** Model rule correction with lineage, versioned approval and replacement/revocation. Enforce two-principal requirement at type level.
- [ ] **EG-DURABLE-KERNEL-R088.2:** Serve correction submission, approval and application through existing rule authority. Test two-principal validation, replacement flow and revocation refusal.
- [ ] **EG-DURABLE-KERNEL-R089.1:** Typed step verification verdict with uncertainty bound and proof binding. Round-trip over wire.
- [ ] **EG-DURABLE-KERNEL-R089.2:** AU consumer path refuses forward-chained conclusions from failed steps. Preserve AU-EG harness integration.
- [ ] **EG-DURABLE-KERNEL-R090.1:** Route GraphOS schema-selection request through EG admission. Validate candidate and bind schema revision/digest.
- [ ] **EG-DURABLE-KERNEL-R090.2:** Serve digest readback with mismatch detection. Test accuracy and consistency across restart.
- [ ] **EG-DURABLE-KERNEL-R090.3:** Enforce read-only on external triple-store projections. Refuse mutations with typed error. Trace lineage to EG revision.
- [ ] **EG-DURABLE-KERNEL-R097.1:** Model typed temporal rule semantics with fact timeline and origin linkage.
- [ ] **EG-DURABLE-KERNEL-R097.2:** Behavior forward-chaining produces facts in time-order and chains derivations.
- [ ] **EG-DURABLE-KERNEL-R097.3:** Wiring fact admission and readback bind origin sentence and axiom references.
- [ ] **EG-DURABLE-KERNEL-R097.4:** Refusal reject rules with missing temporal bounds or malformed axioms.
- [ ] **EG-DURABLE-KERNEL-R097.5:** Evidence durable fact store preserves origin chain and temporal rule instance.
- [ ] **EG-DURABLE-KERNEL-R097.6:** Hot-path proof fact production API returns origin chain; integration test traces derived fact to source axiom.
- [ ] **EG-DURABLE-KERNEL-R098.1:** Model typed ordered argument list and hypothesis-class evidence enum.
- [ ] **EG-DURABLE-KERNEL-R098.2:** Behavior unseen entities route through inductive hypothesis path and bind response.
- [ ] **EG-DURABLE-KERNEL-R098.3:** Wiring hypothesis evidence class is accepted and binds to public hyperedge predictor.
- [ ] **EG-DURABLE-KERNEL-R098.4:** Refusal reject hypotheses with empty argument list and non-hypothesis evidence claims.
- [ ] **EG-DURABLE-KERNEL-R098.5:** Evidence hypothesis output never claims assertion and carries prediction confidence.
- [ ] **EG-DURABLE-KERNEL-R098.6:** Hot-path proof prediction endpoint accepts ordered arguments and routes unseen entity to hypothesis response.
- [ ] **EG-DURABLE-KERNEL-R099 (rollup):** Cross-graph writes fan out across shard writers.
- [ ] **EG-DURABLE-KERNEL-R099.1 (Model):** Add the fan-out report type.
- [ ] **EG-DURABLE-KERNEL-R099.2 (Behavior):** Group sub-batches by shard and submit them concurrently.
- [ ] **EG-DURABLE-KERNEL-R099.3 (Wiring):** Attach the reports in `multi_graph_batch_update`.
- [ ] **EG-DURABLE-KERNEL-R099.4 (Refusal):** Add the size refusal and per-graph outcomes.
- [ ] **EG-DURABLE-KERNEL-R099.5 (Evidence):** Emit reports and shard metrics.
- [ ] **EG-DURABLE-KERNEL-R099.6 (Hot-path proof):** Add the served fan-out test and the benchmark.
- [ ] **EG-DURABLE-KERNEL-R100 (rollup):** Native property-graph mirror sinks.
- [ ] **EG-DURABLE-KERNEL-R100.1 (Model):** Add the three sink kinds.
- [ ] **EG-DURABLE-KERNEL-R100.2 (Behavior):** Implement the three drivers on the mirror-target trait.
- [ ] **EG-DURABLE-KERNEL-R100.3 (Wiring):** Wire registration into `MutationOutbox` and the outbox consumer.
- [ ] **EG-DURABLE-KERNEL-R100.4 (Refusal):** Declare and return the two refusals.
- [ ] **EG-DURABLE-KERNEL-R100.5 (Evidence):** Emit batch receipts and digest reconciliation.
- [ ] **EG-DURABLE-KERNEL-R100.6 (Hot-path proof):** Add the served outage test and the benchmark.

- [ ] **EG-DURABLE-KERNEL-R101 (rollup):** Downtime work is an ontology that drives the engine scheduler (rollup).
- [ ] **EG-DURABLE-KERNEL-R101.1 (Model):** Add the typed model.
- [ ] **EG-DURABLE-KERNEL-R101.2 (Behavior):** Implement the behavior.
- [ ] **EG-DURABLE-KERNEL-R101.3 (Wiring):** Wire the production call site.
- [ ] **EG-DURABLE-KERNEL-R101.4 (Refusal):** Add the typed refusals.
- [ ] **EG-DURABLE-KERNEL-R101.5 (Evidence):** Emit the evidence.
- [ ] **EG-DURABLE-KERNEL-R101.6 (Hot-path proof):** Add the served hot-path test.
