# Tasks

Status key: `[ ]` queued, `[-]` in progress, `[x]` verified at cited main revision. All tasks below remain unchecked until an implementation PR supplies exact evidence.

- [ ] **K-01 (KG-01, KG-02):** Pin the current kernel revision, map every durable write entry point, write the cross-store atomicity ADR, and add all-or-none crash/retry tests (T-KG-01/02).
- [ ] **K-02 (KG-03, KG-04):** Validate and cache owner bindings at one transaction boundary, retain golden layout lineage/refusal cases, implement bounded scrub and typed status (T-KG-03/04).
- [ ] **K-03 (KG-05, KG-06):** Make head-only outbox attempts and reject/rewind semantics observable; add cursor-based CDC/mirror replay, table mirroring, digest reconciliation and empty-default refusal (T-KG-05/06).
- [ ] **K-04 (KG-07):** Enumerate all served read/write paths, thread immutable verified scope and build cross-principal/tenant denial fixtures including cache/UDF/foreign/TSDB (T-KG-07).
- [ ] **K-05 (KG-08, KG-09):** Add durable audit reservation/outcome reconciliation and reserved two-person lease kinds; generate any changed protocol/client contracts (T-KG-08/09).
- [ ] **K-06 (KG-10):** Capture failure traces automatically, restore full multi-group workload, prove repeated loaded failover and independent progress (T-KG-10).
- [ ] **K-07 (KG-11, KG-12):** Add explicit durability classes and loss-window metrics; build recoverable hot structures and linearizable typed primitives (T-KG-11).
- [ ] **K-08 (KG-13, KG-14):** Route point/structure operations before SQL planning, implement one-admission pipeline batches and schema/policy-safe plan cache (T-KG-12).
- [ ] **K-09 (KG-15):** Decide store engine by ADR, add hot/cold/tombstone parity and replication-out tooling, run differential/restore and published benchmark matrix (T-KG-13).
- [x] **K-10 (additional hardening):** Close the native-kernel hardening and test-reliability requirements no task above names: upload-cursor non-reuse, owner-manifest lineage, polygon boundary predicates, ring-buffer eviction identity, dispatch-ordering guard reliability, idempotent CreateGraph/DeleteGraph retry, blob GC grace period, holder-scoped BlobRef/BlobUnref, outbox topic classes, AVX2 kernel coverage, placement/consensus routing after sealing, `.redb` registry classification, mutation-durability classification across feature builds, create-only sealed-record guard, publication-waiter cancellation, shared-chunk GC, legacy-layout migration refusal, bounded test threading, idle-upload reclaim, lossless v2 control-state lift, compiled-shape memoization benchmark, signed-dispatch ack-loss recovery, published-contract/BlobRef parity, isolated trace capture, channel-group leave result and no-key scrub-cursor fixture — **EG-DURABLE-KERNEL-R044–EG-DURABLE-KERNEL-R069** (see `requirements.md` for each ID's definition).
- [ ] **K-11 (all):** Run configured CCCC, Dupehound, jscpd, KISS, rust architecture lint, clippy, focused tests and cloud CI at exact head; record positive and negative evidence, then update delivery and acceptance state separately.
- [-] **K-12 (operator store upgrades):** Give the registered offline store upgrades one operator entry point (`EG-DURABLE-KERNEL-R070`): a single upgrade registry in the storage kernel, an `inspect`/`apply` command on the server binary that holds the engine's own directory lock, a startup refusal that names the command, the operator procedure in the generated owner-store format document, and command tests on disposable predecessor stores.
- [x] **K-13 (lazy agent-library open):** Add the missing startup test for `EG-DURABLE-KERNEL-R006`. Start the real server with the digest-verified binary. Use a directory fixture and a malformed-bytes fixture for `agent_library.redb`. Assert that an authenticated `Ping` succeeds and that an Agent Library operation fails on store open. Assert that `Ping` still succeeds afterward and that the process stays alive. Keep the `EG-DURABLE-KERNEL-R070` upgrade-required boot refusal intact. Recovered from the never-published Codex claim `codex-eg-r006-startup-test-01a101be`.
- [x] **K-14 (R052 acceptance record):** Record the scoped acceptance of `EG-DURABLE-KERNEL-R052` in `status.json`. Cite merge `9871859d4e0cc01f0a5a2f1f07584da056cf424e`, PR 41, its merged-head receipt and CI run 36912559244. Change only the R052 row. Keep R052 delivery at `LANDED` and the whole-spec acceptance at `NOT_AUDITED`. Obtain an independent review of the exact diff first. Recovered from the never-branched Codex claim `codex-r052-acceptance-record-01a0fe5f`.

## Decomposition children (tracked)

- [x] **EG-DURABLE-KERNEL-R011:** Clippy and targeted tests cover the full and all-features builds
- [x] **EG-DURABLE-KERNEL-R011.1:** RequiredTestModuleCategory/RequiredTestModule typed category and validated declaration
- [x] **EG-DURABLE-KERNEL-R014:** Raft cluster tests pass reliably under load
- [x] **EG-DURABLE-KERNEL-R014.1:** Typed flake-budget / retry-policy model for load-test retries
- [x] **EG-DURABLE-KERNEL-R024:** External graph and database backends become EG federation sources
- [x] **EG-DURABLE-KERNEL-R024.1:** PostgreSQL/AGE backend becomes a ForeignSourceSpec source
- [x] **EG-DURABLE-KERNEL-R024.2:** Fan-out backend becomes a MirrorTargetSpec federation mirror target
- [x] **EG-DURABLE-KERNEL-R024.2.1:** MirrorTargetSpec::FanOut typed spec, validation, and registration refusal
- [x] **EG-DURABLE-KERNEL-R024.2.2:** MirrorTargetRegistry registration and name-resolution entry point
- [x] **EG-DURABLE-KERNEL-R024.3:** Cypher transpiler becomes a ForeignSourceSpec source
- [x] **EG-DURABLE-KERNEL-R024.4:** Mirror target becomes a ForeignSourceSpec mirror target
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
- [x] **EG-DURABLE-KERNEL-R040:** Pipelined requests commit as a single batch
- [x] **EG-DURABLE-KERNEL-R040.1:** PipelineBatch/BatchCommitDecision typed model: one admission per batch, order-preserving replies, empty-batch refusal
- [x] **EG-DURABLE-KERNEL-R041:** SQL plan cache keyed by statement digest and schema version
- [x] **EG-DURABLE-KERNEL-R041.1:** PlanCacheKey/PlanCacheLookup typed key and pure lookup decision with stale-schema refusal
- [x] **EG-DURABLE-KERNEL-R041.2:** PlanCache::prepare entry point at the SQL prepare site
- [x] **EG-DURABLE-KERNEL-R042:** Benchmark gate defines and proves performance superiority claims
- [x] **EG-DURABLE-KERNEL-R042.1:** BenchmarkClaim typed report record with refusal for an unpublished or non-passing claim
