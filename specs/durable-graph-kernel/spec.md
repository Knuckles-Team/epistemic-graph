# EG-DURABLE-KERNEL — Durable graph kernel

**Owner:** epistemic-graph · **ID:** `durable-graph-kernel` · **Delivery:** PARTIAL · **Acceptance:** NOT VERIFIED

## State legend

`QUEUED` means no accepted implementation; `BUILT` means implementation evidence exists but landing or final validation is open; `LANDED` means source reached a named main revision; `ACCEPTED` requires the scenarios in this directory against that revision. A decision can be `CLOSED` without product work. These states are independent. This spec itself is a build contract, not evidence that its requirements shipped.

## Purpose and boundary

EG is the system of record for durable graph, table, KV, time series, outbox, audit, and transaction state. A caller may use native, RESP, SQL, or generated client surfaces, but equivalent operations must share one owner-store and policy path. The engine must recover without half-applied writes, prevent principal and tenant data leakage, preserve declared durability, and expose replay/repair evidence. Application orchestration and connector-specific business rules belong to their owning products. Attached-source adapters and virtual-query routing have their own [`unified-data-plane`](../unified-data-plane/spec.md) contract; this spec owns the native kernel and the transaction, durability, audit, replication, and mirror seams it uses.

Actors: application writer, read-only analyst, tenant administrator, replica/mirror consumer, and operator restoring a failed engine. Priority P0 is safe admission, atomic commit, recovery, and security. P1 is durable streaming and replication. P2 is latency and native storage expansion.

## Outcomes and acceptance

1. **P0 single authority:** A writer submits an authenticated MutationBatch. Admission checks owner scope, policy, idempotency, storage lineage and transaction compatibility before any durable effect. An acknowledged sync commit survives restart exactly once; a rejected or failed commit leaves no partial graph/table/KV/TSDB state.
2. **P0 isolation:** A principal cannot read or mutate another principal's private rows, foreign source, UDF, cache key, audit reservation, or stream subscription merely by knowing its name. A tenant mismatch fails with a stable access error. Explicit share grants are revocable and are checked at use time.
3. **P0 recovery:** On restart, the engine validates owner manifests and format lineage, replays committed state only, detects corruption by bounded scrub, and reports typed findings. Unsupported on-disk layouts fail with a named upgrade-required error; they are never silently reinterpreted.
4. **P1 observability:** Outbox/CDC consumers have durable cursor and head-only attempt accounting. A failing head cannot exhaust the retry budget of healthy successors. Reject, dead-letter, rewind, replay and reconcile are visible and auditable; mirrors have a non-empty-default guard.
5. **P1 replication:** A multi-group cluster preserves independent leader progress and a committed batch's exactly-once observable result across leader failover. Failure traces explain heartbeat, append, and recovery decisions without requiring a live operator to toggle tracing.
6. **P2 native performance:** Point and data-structure operations use native KV/GraphCore execution across wires; SQL uses a schema-versioned plan cache. Durability is explicit as ephemeral, async, or sync. Any speed comparison names workload, class, hardware and measured p99; no blanket database superiority claim is accepted.

## Normative requirements

| ID | Requirement | Stable work IDs | Acceptance evidence |
|---|---|---|---|
| KG-01 | MutationBatch is the sole persistent write envelope. All store members use the owner manifest, one admission decision and one idempotent terminal outcome. | EG-DURABLE-KERNEL-R043, EG-DURABLE-KERNEL-R012, EG-DURABLE-KERNEL-R023 | restart/replay and mixed-member tests on a main revision |
| KG-02 | Transaction commit is atomic across all participating members, or explicitly refuses an unsupported mixed transaction before effects. Table-only transactions never enter the mixed-store path. A chosen cross-store mechanism has an ADR and fault-point proof. | EG-DURABLE-KERNEL-R034 | crash matrix and concurrent anomaly corpus |
| KG-03 | Owner manifest validation occurs at manifest read and at the write transaction boundary. Cache a validated binding only within that transaction. Format changes have versioned lineage and typed upgrade refusal. | EG-DURABLE-KERNEL-R003, EG-DURABLE-KERNEL-R021 | lineage golden fixtures and stale-binding negative tests |
| KG-04 | A bounded, resumable, read-only content scrub reports typed corruption findings without blocking writers or treating per-commit graph unseal as a scrub. | EG-DURABLE-KERNEL-R020 | injected corruption/restart/rate-limit tests |
| KG-05 | The outbox increments an attempt only for the attempted head, supports consumer reject and operator rewind, and never acknowledges a skipped reasoning projection without a rebuild. | EG-DURABLE-KERNEL-R001, EG-DURABLE-KERNEL-R002, EG-DURABLE-KERNEL-R004, EG-DURABLE-KERNEL-R005 | failing-head and successor replay tests |
| KG-06 | CDC and mirrors publish only committed outcomes, use stable per-consumer cursors, replay idempotently after outage, and compare source and target digests before declaring caught up. Mirror reads cannot become an alternate authority. | EG-DURABLE-KERNEL-R024, EG-DURABLE-KERNEL-R036 | outage, replay, drift and empty-target tests |
| KG-07 | Every read/write path carries verified tenant and principal scope through RLS, SQL, UQL, TSDB, KV, foreign sources, UDFs and caches. Cross-scope lookup fails closed; sharing requires an explicit grant. Foreign-source sharing is specified by `federated-query-and-reasoning`; app consolidation policy is specified by `unified-data-plane`. | EG-DURABLE-KERNEL-R018, EG-DURABLE-KERNEL-R019, EG-DURABLE-KERNEL-R029 | served cross-principal and cross-tenant denial matrix |
| KG-08 | Governed effects reserve a tenant-scoped idempotent audit record before the effect and close it with linked outcome. An unavailable writer or missing audit class blocks the effect, and interrupted outcomes reconcile to a truthful state. | EG-DURABLE-KERNEL-R031 | duplicate request, outage, crash and mismatch tests |
| KG-09 | Temporary elevation and schema repair use separate reserved lease kinds with two distinct verified principals, exact request digest, expiry and audit; generic lease write cannot grant either. | EG-DURABLE-KERNEL-R022, EG-DURABLE-KERNEL-R027 | proposer=approver, wrong digest, expired and replay refusals |
| KG-10 | Raft dispatch preserves per-peer and per-group progress. Multi-node tests capture causal traces on failure and repeatedly prove leader failover and multi-group writes without sleep/retry/assertion relaxation. | EG-DURABLE-KERNEL-R008, EG-DURABLE-KERNEL-R009, EG-DURABLE-KERNEL-R013, EG-DURABLE-KERNEL-R014, EG-DURABLE-KERNEL-R015, EG-DURABLE-KERNEL-R016, EG-DURABLE-KERNEL-R025 | repeated loaded cluster tests and trace artifact |
| KG-11 | Namespace/table durability is declared, queryable and never downgraded: `sync` commit-before-ack default; `async` measured loss window ≤100 ms; `ephemeral` RAM+TTL and lost on restart. Authz and principal-bound keyspace apply to all classes. | EG-DURABLE-KERNEL-R038 | kill-point and measured-loss tests |
| KG-12 | In-memory hash, sorted set, list and set structures are working state only. The redb log/recovery rules remain the durable authority for async/sync. Native atomic primitives provide `INCRBY`+expiry, `SET NX EX`, CAS lease, token bucket, bounded counter and work claim. | EG-DURABLE-KERNEL-R039 | concurrent linearizability/rebuild tests |
| KG-13 | RESP and SQL extended-protocol batches receive one admission and one group commit with ordered N replies. A failed member cannot yield earlier success replies implying durability. | EG-DURABLE-KERNEL-R040 | partial failure and response-order tests |
| KG-14 | The native route handles point/structure requests without a DataFusion session. SQL plans are keyed by statement digest and schema version and invalidated with the existing dependency clock. | EG-DURABLE-KERNEL-R037, EG-DURABLE-KERNEL-R041 | route and invalidation tests |
| KG-15 | Native row hot/cold union, store-engine choice, replication-out and operational compatibility are admitted only with differential, restore and workload evidence; no mirror or external cache becomes a read authority. | EG-DURABLE-KERNEL-R032, EG-DURABLE-KERNEL-R033, EG-DURABLE-KERNEL-R035, EG-DURABLE-KERNEL-R042 | ADR, corpus, restore drill and benchmark report |

## Scope and dependencies

`KG-01`–`KG-10` consolidate kernel obligations that this spec covers directly; `KG-11`–`KG-15` are queued native-storage work not yet accepted. Serving clients and finance workloads exercise this kernel but do not move their orchestration or accounting into it. The attached-source registry, CDC adapters, dialects and query federation remain in `unified-data-plane`; both specs use the same ChangeEnvelope, position and audit contracts. Owner-specific app cutover is never implied by this spec.

For every implementation PR, link each changed `KG-*` requirement, cite the exact commit and CI run, and update delivery and acceptance separately. Source committed to a branch is `BUILT`, even if focused tests pass. `LANDED` needs a main ancestor; `ACCEPTED` needs the matching positive and negative tests at that head. No historical delivery label alone satisfies acceptance.

### Detailed migration crosswalk

This table makes the historical work identifiers searchable without requiring their drafts. It records the present migration classification, not an acceptance verdict. Where an old item was a specific gate repair, the durable requirement below is the continuing contract.

| IDs | Current classification | Normative destination here |
|---|---|---|
| EG-DURABLE-KERNEL-R043. EG-DURABLE-KERNEL-R001, EG-DURABLE-KERNEL-R002, EG-DURABLE-KERNEL-R003, EG-DURABLE-KERNEL-R004, EG-DURABLE-KERNEL-R005, EG-DURABLE-KERNEL-R006, EG-DURABLE-KERNEL-R007 | source landed or built. Acceptance unverified | KG-01, KG-03, KG-05. Owner layout, outbox and local delivery state |
| EG-DURABLE-KERNEL-R008, EG-DURABLE-KERNEL-R009, EG-DURABLE-KERNEL-R012, EG-DURABLE-KERNEL-R013, EG-DURABLE-KERNEL-R014, EG-DURABLE-KERNEL-R015, EG-DURABLE-KERNEL-R016, EG-DURABLE-KERNEL-R025 | source landed or built. Loaded repeat proof open | KG-01, KG-10. Raft command, leader forwarding and durability |
| EG-DURABLE-KERNEL-R020, EG-DURABLE-KERNEL-R021, EG-DURABLE-KERNEL-R017 | built; acceptance unverified | KG-03, KG-04, KG-15; scrub, binding and correctness benchmark |
| EG-DURABLE-KERNEL-R018, EG-DURABLE-KERNEL-R019, EG-DURABLE-KERNEL-R022, EG-DURABLE-KERNEL-R026, EG-DURABLE-KERNEL-R027, EG-DURABLE-KERNEL-R028, EG-DURABLE-KERNEL-R029, EG-DURABLE-KERNEL-R030 | landed, built or queued by item. Acceptance unverified | KG-07, KG-09. Scope, auth mode, RLS and lease denial |
| EG-DURABLE-KERNEL-R023, EG-DURABLE-KERNEL-R031, EG-DURABLE-KERNEL-R034, EG-DURABLE-KERNEL-R035, EG-DURABLE-KERNEL-R036 | built or queued. Acceptance unverified | KG-02, KG-06, KG-08, KG-15. Readonly operation classification, atomicity, audit, mirrors and tooling |
| EG-DURABLE-KERNEL-R037, EG-DURABLE-KERNEL-R038, EG-DURABLE-KERNEL-R039, EG-DURABLE-KERNEL-R040, EG-DURABLE-KERNEL-R041, EG-DURABLE-KERNEL-R042 | queued | KG-11–KG-15; native routing, durability, hot state, batch and measurement |
| EG-DURABLE-KERNEL-R010, EG-DURABLE-KERNEL-R011 | gate source landed or deferred | Quality proof: differential clones and feature-specific Rust coverage; no runtime feature claimed |

The method-codec artifact EG-CONTRACT-R006 belongs to the [public engine contract and release spec](../public-engine-contract-and-release/spec.md). It does not define a second security mechanism here. Pack-specific manifest refusal EG-TYPED-PACKS-R056 belongs to [typed packs and catalog](../typed-packs-and-catalog/spec.md). User-managed index lifecycle EG-FEDERATED-QUERY-R009 and foreign-source grants EG-FEDERATED-QUERY-R013/EG-FEDERATED-QUERY-R014 belong to [federated query and reasoning](../federated-query-and-reasoning/spec.md). Attached-source acceleration EG-UNIFIED-DATA-PLANE-R010 and app isolation EG-UNIFIED-DATA-PLANE-R031 belong to [unified data plane](../unified-data-plane/spec.md). These references are integration boundaries, not duplicate owner claims.

## Quality and exclusion rules

Reuse `crates/eg-types` MutationBatch, `crates/eg-transaction` admission, `crates/eg-storage` owner manifest, existing redb and Raft stores, `src/server` auth/wires and `eg-tsdb`/`eg-stream`. No parallel persistent log store, cache product, alternate write authority, or unguarded fast path. Apply the repository's configured CCCC changed-function gate (no new function over cyclomatic 10 or cognitive 15), Dupehound structural clone gate, jscpd differential gate, KISS changed-Rust gate, Rust architecture lint, formatting, clippy and relevant tests. The CI gate must be runnable on an ephemeral cloud runner with declared fixture provisioning; live private services are an optional deployment proof, never the sole code acceptance route.

Requirement IDs are defined in [requirements.md](requirements.md); delivery state per ID is in `status.json`.

## Worker registry producer boundary

R074.1 owns the pure typed capacity and liveness transitions in eg-types. R074.2 owns authenticated served dispatch, ownership enforcement and generated clients. R074 remains open until both children satisfy their tests; the producer alone does not serve a registry.

## Scheduler config producer boundary

R076 is split into R076.1 and R076.2. The first owns the pure typed policy and validation in `eg-types`. The second must expose and consume it through the real engine setting/admission paths. Model tests do not establish a served setting or complete R076. EG-PLACEMENT-R002.2 extends the producer contract with autosize defaults and queue depth.

## Required runtime manifest boundary

R011.2 owns the concrete crate, feature, and module-filter declarations in `eg-types`.
The declaration refuses an empty discovered selection.
R011.3 owns pre-merge execution; R011.4 owns exact-head CI acceptance receipts.
Manifest unit tests do not establish runtime acceptance for R011.

## Safe eviction and durable read-through boundary

EG-DURABLE-KERNEL-R086 records the existing node-residency and durable point-read contract.
It adds explicit cache-miss, scope, refusal and restart evidence under this durability owner.
R039 rebuild acceptance and all existing receipts remain unchanged.
A registry reopen, resident read or declared seam is insufficient proof of safe eviction.
This definition batch changes no runtime behavior and claims no completed R086 implementation.
Lifecycle state comes only from generated status.json.

<!-- spec-definition-patch:owner-batch-20261012-durable-graph-kernel -->
## Reviewed owner residuals — 2026-10-12

### `EG-DURABLE-KERNEL-R028.1` — AU:21

Original objective: Production agent runs checkpoint durably across process boundaries, separate short-term from long-term memory, isolate tenants and stream output.

EG-owned residual: The complete agent-run checkpoint payload and cross-process resume binding are not defined by a WorkItem lifecycle test. Short-term versus long-term memory lifecycle/retention and resumed-output binding to that run/memory state remain unspecified.

Producer/consumer boundary: EG WorkItem lifecycle and isolation -> AU run execution -> existing GRAPHOS-A2A-R001/GRAPHOS-OPS-R001 streaming facade. The reviewed packet has no explicit dependency joining a complete run-checkpoint/memory contract to the resume and stream consumers.

Preserve existing coverage: `EG-DURABLE-KERNEL-R028`, `EG-IDENTITY-R006`, `GRAPHOS-A2A-R001`, `GRAPHOS-A2A-R001.1`, `GRAPHOS-OPS-R001`.

Owner source hash: `d89bb9c69e2fdef6a5c3fd428ede3f91868539056c2b949700867d8d8087d1e7`. This definition adds no AU/GraphOS/SDK implementation or acceptance claim.

### `EG-DURABLE-KERNEL-R085.9` — AU:48

Original objective: A search run keeps its state (transcript, evidence, constraints, open claims) in an externalized store separate from decision logic, so context growth does not erase evidence.

EG-owned residual: No search-state record explicitly combines transcript, evidence, constraints and open claims; no continuation contract restores all four from authoritative external state after context eviction.

Producer/consumer boundary: AU summary/workspace consumers and EG authenticated durable memory dispatch exist as definitions. Search continuation has no named binding to a complete search-state revision.

Preserve existing coverage: `AU-HARNESS-R013.13`, `AU-HARNESS-R020.1`, `EG-DURABLE-KERNEL-R085.1`, `EG-DURABLE-KERNEL-R085.4`, `EG-DURABLE-KERNEL-R085.7`.

Owner source hash: `8c76fa1637e0cc2082a1cce16b9d637f12b310ae96d1fe9a50ec9c64f8e23944`. This definition adds no AU/GraphOS/SDK implementation or acceptance claim.

### `EG-DURABLE-KERNEL-R085.10` — AU:49

Original objective: The harness stores and retrieves notes on prior sessions and research context transparently, so users need not repeat requirements.

EG-owned residual: Prior-session notes/research context are not automatically captured and reused for the current user/task. Storage conflict retention does not define how stale or conflicting notes influence transparent contextual reuse.

Producer/consumer boundary: EG authorized memory read/list -> AU context consumer; AU experience retrieval handles reusable run records. Automatic research-note capture/relevance/application is not wired by a definition.

Preserve existing coverage: `AU-HARNESS-R022.3`, `EG-DURABLE-KERNEL-R085.2`, `EG-DURABLE-KERNEL-R085.5`, `EG-DURABLE-KERNEL-R085.7`.

Owner source hash: `2362fd76bf40223aeb813866392670b65eae13d02a725b1af9a2e6711542dd12`. This definition adds no AU/GraphOS/SDK implementation or acceptance claim.

### `EG-DURABLE-KERNEL-R084.7` — EG:28

Original objective: A multi-tenant graph coordinates concurrent writers across backends so that tenant writes serialize deterministically; a write that cannot be coordinated is rejected with a typed conflict.

EG-owned residual: Single-graph optimistic version arbitration does not establish deterministic tenant-wide ordering across backends, including coordinated failure behavior. R077 exposes width only. R024/R036 treat external backends as federation/mirror targets, while R039 retains one durability authority; these boundaries must not be silently replaced by distributed writes to read-only sources.

Producer/consumer boundary: Served ApplyMutation -> version check/resolve_conflict -> durable mutation authority -> declared mirror targets where supported. R024.1 is read-only; R036 supplies mirror replay/cursors, not distributed transaction coordination.

Preserve existing coverage: `EG-DURABLE-KERNEL-R084.1`, `EG-DURABLE-KERNEL-R084.3`, `EG-DURABLE-KERNEL-R084.4`, `EG-DURABLE-KERNEL-R084.6`, `EG-DURABLE-KERNEL-R084.2`, `EG-DURABLE-KERNEL-R084.5`.

Owner source hash: `96c3079859b0e63ec3e1174e25e7a4ab107b23ef6863ae9b54ff0e921a88e4b0`. This definition adds no AU/GraphOS/SDK implementation or acceptance claim.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:final-mappings-20261012 -->
## Final mapped contract extensions — 2026-10-12

These definitions are planned contracts; they do not claim implementation or served evidence.

### `EG-DURABLE-KERNEL-R038.100` — EG:60

Define an explicit deployment profile with `embedded` (in-process operation requiring no external service) and `served` (configured service endpoint) modes. Profile selection controls transport/startup only; it does not silently change the existing persistent-namespace durability default or sync/redb acknowledgment authority. Ephemeral in-memory namespaces require an explicit `ephemeral` declaration and retain no durable log; persistent namespaces retain their declared `sync` or `async` class. Do not adopt the source request’s universal in-memory default.

**Producer/consumer boundary:** EG owns profile and storage selection; transport mode is separate from namespace DurabilityClass. Preserve R038 durability classes, sync default, redb authority and authorization/HMAC isolation.

**Source record SHA-256:** `b36ed0f40742d2f455b4eae67f2fb64edfb7a6ea87b93cbb8ac0046970728458`.

**Preserved parent:** `EG-DURABLE-KERNEL-R038` remains unchanged.
<!-- /spec-definition-patch -->


<!-- spec-definition-patch:owner-dispatch-20261012-spec -->
## Cross-owner committed change feed

`ApplyChangeEnvelope` remains the sole commit authority. The proposed `OpenGraphSnapshotV1` returns a graph-incarnation/revision snapshot digest and an exact high-water cursor from the same read. `ReadGraphChangesV1` returns ordered commit/event-ordinal records, including authorized tombstones, using an opaque cursor bound to tenant, graph and authorization revision. It provides stable ids for replay deduplication, a 1..256 page bound, 24-hour retention, `CURSOR_EXPIRED` recovery and `AUTHORIZATION_CHANGED` refusal. Every delivered event exposes versioned commit-to-visible timing evidence; served p95 target is ≤1000 ms from commit acknowledgement to first authorized read.

The feed is read-only over durable commits, not an alternative write envelope or a local browser echo. GraphOS routes the operations to WebUI R030.8; the paired two-client test verifies reconnect/order and independent graph readback. The consumer's readable layout remains a separate WebUI obligation.
<!-- /spec-definition-patch -->

## Reviewed benchmark measurement extension — `EG-DURABLE-KERNEL-R042.100`

The existing `BenchmarkClaim.measured_result` gains a typed `BenchmarkMeasurementV1` binding a benchmark configuration/workload, dataset/query digest, release commit, model/provider/tokenizer revisions, hardware/profile settings, model-ready and request timestamps, complete input-token count, inference elapsed time and pinned price table. Throughput uses all input tokens divided by post-ready inference time. Cost is calculated per query from the pinned prices and currency exponent with a specified rounding rule. Startup is reported but never included in inference time or throughput. Only identical workload/profile measurements are comparable. Missing tokenizer/token count/price, invalid elapsed time, zero denominator or mismatched profiles refuse claim admission. This extends the existing claim-validation record and does not introduce another benchmark runner. It remains SPECIFIED; no performance result is asserted.
