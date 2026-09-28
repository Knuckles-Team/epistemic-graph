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
| KG-01 | MutationBatch is the sole persistent write envelope; all store members use the owner manifest, one admission decision and one idempotent terminal outcome. | RF-025, EH-256, EH-446 | restart/replay and mixed-member tests on a main revision |
| KG-02 | Transaction commit is atomic across all participating members, or explicitly refuses an unsupported mixed transaction before effects. Table-only transactions never enter the mixed-store path. A chosen cross-store mechanism has an ADR and fault-point proof. | EH-690 | crash matrix and concurrent anomaly corpus |
| KG-03 | Owner manifest validation occurs at manifest read and at the write transaction boundary; cache a validated binding only within that transaction. Format changes have versioned lineage and typed upgrade refusal. | EH-140, EH-150, EH-390 | lineage golden fixtures and stale-binding negative tests |
| KG-04 | A bounded, resumable, read-only content scrub reports typed corruption findings without blocking writers or treating per-commit graph unseal as a scrub. | EH-384 | injected corruption/restart/rate-limit tests |
| KG-05 | The outbox increments an attempt only for the attempted head, supports consumer reject and operator rewind, and never acknowledges a skipped reasoning projection without a rebuild. | EH-138, EH-139, EH-147, EH-159 | failing-head and successor replay tests |
| KG-06 | CDC and mirrors publish only committed outcomes, use stable per-consumer cursors, replay idempotently after outage, and compare source and target digests before declaring caught up. Mirror reads cannot become an alternate authority. | EH-508, EH-692 | outage, replay, drift and empty-target tests |
| KG-07 | Every read/write path carries verified tenant and principal scope through RLS, SQL, UQL, TSDB, KV, foreign sources, UDFs and caches. Cross-scope lookup fails closed; sharing requires an explicit grant. | EH-373, EH-374, EH-375, EH-378, EH-640, EH-695 | served cross-principal and cross-tenant denial matrix |
| KG-08 | Governed effects reserve a tenant-scoped idempotent audit record before the effect and close it with linked outcome; an unavailable writer or missing audit class blocks the effect, and interrupted outcomes reconcile to a truthful state. | EH-658 | duplicate request, outage, crash and mismatch tests |
| KG-09 | Temporary elevation and schema repair use separate reserved lease kinds with two distinct verified principals, exact request digest, expiry and audit; generic lease write cannot grant either. | EH-404, EH-560 | proposer=approver, wrong digest, expired and replay refusals |
| KG-10 | Raft dispatch preserves per-peer and per-group progress. Multi-node tests capture causal traces on failure and repeatedly prove leader failover and multi-group writes without sleep/retry/assertion relaxation. | EH-178, EH-181, EH-286, EH-287, EH-288, EH-290, EH-534 | repeated loaded cluster tests and trace artifact |
| KG-11 | Namespace/table durability is declared, queryable and never downgraded: `sync` commit-before-ack default; `async` measured loss window ≤100 ms; `ephemeral` RAM+TTL and lost on restart. Authz and principal-bound keyspace apply to all classes. | EH-718 | kill-point and measured-loss tests |
| KG-12 | In-memory hash, sorted set, list and set structures are working state only; the redb log/recovery rules remain the durable authority for async/sync. Native atomic primitives provide `INCRBY`+expiry, `SET NX EX`, CAS lease, token bucket, bounded counter and work claim. | EH-719 | concurrent linearizability/rebuild tests |
| KG-13 | RESP and SQL extended-protocol batches receive one admission and one group commit with ordered N replies; a failed member cannot yield earlier success replies implying durability. | EH-720 | partial failure and response-order tests |
| KG-14 | The native route handles point/structure requests without a DataFusion session; SQL plans are keyed by statement digest and schema version and invalidated with the existing dependency clock. | EH-717, EH-721 | route and invalidation tests |
| KG-15 | Native row hot/cold union, store-engine choice, replication-out and operational compatibility are admitted only with differential, restore and workload evidence; no mirror or external cache becomes a read authority. | EH-684, EH-685, EH-691, EH-722 | ADR, corpus, restore drill and benchmark report |

## Scope and dependencies

`KG-01`–`KG-10` consolidate kernel obligations that appear across trains T1–T6; `KG-11`–`KG-15` are queued T9 native-storage work. T7 serving clients and T8 finance workloads exercise this kernel but do not move their orchestration or accounting into it. The T9 attached-source registry, CDC adapters, dialects and query federation remain in `unified-data-plane`; both specs use the same ChangeEnvelope, position and audit contracts. Owner-specific app cutover is never implied by this spec.

For every implementation PR, link each changed `KG-*` requirement and relevant `EH-*` or `RF-*` ID, cite the exact commit and CI run, and update delivery and acceptance separately. Source committed to a branch is `BUILT`, even if focused tests pass. `LANDED` needs a main ancestor; `ACCEPTED` needs the matching positive and negative tests at that head. No historical ledger label alone satisfies acceptance.

### Detailed migration crosswalk

This table makes the historical work identifiers searchable without requiring their drafts. It records the present migration classification, not an acceptance verdict. Where an old item was a specific gate repair, the durable requirement below is the continuing contract.

| IDs | Current classification | Normative destination here |
|---|---|---|
| RF-025; EH-138, EH-139, EH-140, EH-147, EH-150, EH-159, EH-160, EH-169 | source landed or built; acceptance unverified | KG-01, KG-03, KG-05; owner layout, outbox and local delivery state |
| EH-178, EH-181, EH-256, EH-286, EH-287, EH-288, EH-290, EH-534 | source landed or built; loaded repeat proof open | KG-01, KG-10; Raft command, leader forwarding and durability |
| EH-384, EH-390, EH-352, EH-353 | built; acceptance unverified | KG-03, KG-04, KG-15; scrub, binding, durable index and correctness benchmark |
| EH-373, EH-374, EH-375, EH-378, EH-404, EH-535, EH-560, EH-639, EH-640, EH-649 | landed, built or queued by item; acceptance unverified | KG-07, KG-09; scope, auth mode, RLS and lease denial |
| EH-446, EH-658, EH-690, EH-691, EH-692, EH-695 | built or queued; acceptance unverified | KG-02, KG-06, KG-08, KG-15; readonly operation classification, atomicity, audit, mirrors and tooling |
| EH-717, EH-718, EH-719, EH-720, EH-721, EH-722 | queued | KG-11–KG-15; native routing, durability, hot state, batch and measurement |
| EH-185, EH-191 | gate source landed or deferred | Quality proof: differential clones and feature-specific Rust coverage; no runtime feature claimed |

The generic method-codec artifact size item EH-383 belongs to the [public engine contract and release spec](../public-engine-contract-and-release/spec.md); it does not define a second security mechanism here. The accelerated attached-source copy EH-669 belongs to [unified data plane](../unified-data-plane/spec.md), because its source of truth is external. This boundary keeps native storage and attached-source acceleration distinct.

## Quality and exclusion rules

Reuse `crates/eg-types` MutationBatch, `crates/eg-transaction` admission, `crates/eg-storage` owner manifest, existing redb and Raft stores, `src/server` auth/wires and `eg-tsdb`/`eg-stream`. No parallel persistent ledger, cache product, alternate write authority, or unguarded fast path. Apply the repository's configured CCCC changed-function gate (no new function over cyclomatic 10 or cognitive 15), Dupehound structural clone gate, jscpd differential gate, KISS changed-Rust gate, Rust architecture lint, formatting, clippy and relevant tests. The CI gate must be runnable on an ephemeral cloud runner with declared fixture provisioning; live private services are an optional deployment proof, never the sole code acceptance route.
