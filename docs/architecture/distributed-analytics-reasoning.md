# Distributed analytics and incremental reasoning

The main graph-os build includes a durable analytics worker plane and a
change-driven reasoning projection. Both consume committed engine state and use
opaque operational identities; neither copies runtime principals, prompts,
credentials, endpoints, user names, or filesystem locations into coordinator
records. The compact reasoning sidecar additionally hashes graph identifiers;
the authoritative graph remains the only source for their original form.

## AnalyticsJob lifecycle

```text
Submitted -> Running(checkpoint, renewable fenced lease)
          -> Publishing(complete typed result is durable)
          -> Succeeded(claim MutationBatch committed)
```

The owning Raft placement group is the single scheduling authority. Each replica
maintains the same deterministic `jobs.redb` state-machine projection, and snapshots
retain the native command history needed to recover it. Submission ids derive from
the committed batch identity rather than a process counter, so replay and follower
apply converge. There is no pod-number or singleton-coordinator election.

A bounded colocated pool (`EG_ANALYTICS_WORKERS`, default `1`) may execute jobs in a
single-node deployment; clustered authorities disable colocated execution and use
remote workers so a scheduler leader change never strands an in-process kernel.
Remote workers atomically lease the highest-priority eligible job through Raft. The
store transaction checks per-tenant active/CPU quotas, worker pool/region/capabilities,
and durable retry backoff; the leased worker enforces the deadline and CPU budget while
the kernel runs. The scheduler increments a lease epoch that fences late workers.
Workers renew leases and use the same epoch for checkpoints, cancellation, result
staging and publication.

Remote workers use `Method::AnalyticsJob` with the following `JobOp` variants:

| Operation | Required fields | Result |
|---|---|---|
| `WorkerClaim` | `worker_instance`, capabilities, lease duration | `null`, or `{job, lease}` including the pseudonymized compute payload |
| `WorkerRenew` | job id, worker instance, lease epoch, lease duration | renewed lease |
| `WorkerCheckpoint` | job id, worker instance, epoch, progress, enumerated stage, optional opaque state ref | redacted job status |
| `WorkerStage` | job id, worker instance, epoch, complete typed result | `Publishing` job status |
| `WorkerPublish` | job id, worker instance, epoch | terminal job status after transactional claim publication |
| `WorkerFail` | job id, worker instance, epoch, enumerated reason code | retry/backoff or terminal failure status |
| `WorkerCancel` | job id, worker instance, epoch | terminal cancellation acknowledgement |

Every worker operation requires a verified `eg2.` RequestContext with both the
normal mutation grant and the dedicated `analytics:worker` scope (or
`kg:admin`). The server derives the durable worker reference by hashing the
verified principal with `worker_instance`; callers never choose the stored
identity. A worker should generate a random opaque instance value per concurrent
slot and reuse it only when retrying that slot. Claim, stage, publish and cancel
responses are idempotent for the same authenticated slot and epoch. Lease
durations are clamped to 1–300 seconds, and quotas remain server-owned.

The companion `agent-utilities` distribution exposes the standalone entry point
`graph-os-analytics-worker`. It requires `GRAPH_SERVICE_ENDPOINTS` (or
`GRAPH_SERVICE_TCP_ADDR`), `GRAPH_SERVICE_AUTH_SECRET`,
`GRAPH_OS_ANALYTICS_PRINCIPAL`, `GRAPH_OS_ANALYTICS_TENANT`,
`AUTH_JWT_AUDIENCE`, and `KG_POLICY_VERSION`. Slot, lease and poll bounds can be
set with `EG_ANALYTICS_WORKER_SLOTS`, `EG_ANALYTICS_WORKER_LEASE_MS`, and
`EG_ANALYTICS_WORKER_POLL_SECONDS`; optional placement advertisements use the
comma-separated `EG_ANALYTICS_WORKER_CAPABILITIES` (default
`mining.association,pool:default`). These values are runtime configuration and
secrets, never persisted in a job or result. The native TCP protocol is signed
with the shared HMAC secret; a deployment that requires transport mTLS must
actually terminate and enforce it in a service mesh or authenticated proxy
rather than treating a mounted certificate as enforcement.

Association mining checks a cooperative cancellation token inside transaction
scans, candidate/rule generation, and Eclat/FP-growth recursion. CPU/deadline
limits therefore interrupt the kernel instead of waiting for an entire batch.
Output-byte limits are enforced before result staging. Memory/IO values are
scheduler reservations for placement/admission.

The result is an immutable typed `KnowledgeBatch`: schema, every output row,
evidence/source/proof/contradiction references, uncertainty/calibration and a
reproducibility manifest. Its input identity includes both the pinned graph
version and actual input-content digest. The result dataset and `Publishing`
transition commit together. One aggregate claim plus one evidence-bearing claim
per result row is then lowered to graph methods and committed through the
authoritative MutationBatch gateway. A claim failure leaves the job in
`Publishing` for leased replay; it can never report success early or discard
computed rows.

Publication is an explicit cross-group saga when the result graph is not owned by
the scheduler group. The scheduler first returns a bounded, encrypted prepare plan
after validating the live worker lease. The coordinator freezes the current target
placement epoch/fence, commits the deterministic claim MutationBatch through that
target graph's Raft group, and only then submits a compact finalize receipt to the
scheduler group. Target commit and scheduler finalize are independently idempotent.
Finalization accepts the prepared worker epoch after lease expiry (the target commit
may outlive the lease) but rejects a newer epoch, a different result reference, or a
changed placement. A retry after either acknowledgement loss therefore converges
without reporting `Succeeded` before the graph commit.

Ready, active and publishing gauges contain aggregate counts only. The
authority refreshes them as leases age even with colocated execution disabled,
so an autoscaler can recover work after a remote worker disappears. An expired
cancellation awaiting its terminal reconciliation is also counted as ready
coordinator work; job, tenant and worker references never become metric labels.

The association worker result schema is exact and closed: `id` and `kind` are
strings; `confidence`, `support`, and `lift` are float64; and `evidence_refs`,
`source_refs`, `proof_ids`, `contradiction_ids`, `antecedent`, and `consequent`
are string lists. Every column is non-null, reference values are opaque digest
identifiers, and extra/free-text row fields are rejected. The reproducibility
manifest must exactly match the leased input dataset/content/version,
`family:algorithm`, parameters, implementation/environment, and policy
fingerprint. `WorkerFail` accepts only `kernel_cancelled`, `kernel_failure`,
`deadline_exceeded`, `cpu_budget_exceeded`, or `invalid_payload`.

## Incremental reasoning

Every authoritative MutationBatch emits an ordered outbox record. After graph
recovery, the reasoning worker:

1. leases pending records with an epoch;
2. resolves the immutable authoritative batch and verifies the wake-up's operation
   digest, then applies its privacy-safe delta (domain-separated identity hashes and
   closed categorical tags only) to the compact support/conflict/causal/materialization
   index;
3. atomically replaces the compact projection snapshot; and
4. acknowledges that exact lease, advancing the durable projection cursor in
   the same backend transaction.

A crash before acknowledgement replays safely. A missing projection snapshot is
bootstrapped once from the recovered graph; steady-state maintenance is strictly
change-driven. A present but corrupt snapshot fails closed and requires repair—it is
never replaced by an apparently valid empty index. Contradictions remain explicit
conflict edges, so unrelated facts are never inferred or retracted (paraconsistency).
Dependency, policy, model, and ontology changes transitively mark only affected
materializations stale.

Snapshot recovery checks the file-size ceiling before allocation and performs an
allocation-free MessagePack item/depth preflight before deserialization. Publication
streams MessagePack directly to a cap-enforcing same-directory temporary writer; the
writer refuses a chunk before it could cross the byte ceiling, so no whole-snapshot
output buffer exists. The image is fsync'd and published with the platform's atomic
replacement primitive, including replacement of an existing image on Windows; an
encode or swap failure removes the temporary image and leaves the prior image
authoritative. Every `DERIVED_FROM` and `GENERATED_BY` target is an invalidation
dependency. If source data supplies multiple `GENERATED_BY` targets, the projection
range-seeks to that materialization's ordered provenance prefix, deterministically
indexes its first opaque generator, and validates the corresponding reverse index on
recovery.

The persisted snapshot, outbox row, and cursor use the current required schema.
Graph events carry a positive source graph version, and both the compact index and
durable acknowledgement reject watermark regression or a different batch claiming
the same graph version. Unsupported snapshot shapes fail closed; only an absent
snapshot triggers authoritative bootstrap.

`MaterializationStatus` and `StaleMaterializations` read only this durable per-graph
authority. `RecomputeMaterialization` requires an expected source graph version and
request data cannot inject dependencies or generators. In a cluster, the graph group
first commits a deterministic empty-row-delta fence that advances the authoritative
version and emits an opaque recompute wake-up. The response is explicitly `Queued`
with `projection_pending=true`; the projection worker then resolves provenance from
the committed graph post-image, claims/completes its monotonic fence, fsyncs the new
sidecar, and only then acknowledges the outbox cursor. A single-node deployment may
perform the same recompute synchronously and returns `projection_pending=false`.

The reasoning worker is intentionally colocated with the graph authority: it must
read the private authoritative batch/snapshot backend before acknowledging the cursor.
Deploy it as part of the graph-os stateful authority tier, not as a remote stateless
worker.

## Shared control projections

`admin-mutations.redb` is a local projection, not a pod-local coordinator. Native
transaction-control, session-control, and cluster-admin commands that can update it
are serialized by the placement/control Raft group. Successful encrypted native
commands are carried in that group's snapshot history, so a new replica rebuilds the
same control projection during snapshot installation. A transaction's original data
graph is frozen inside `BeginTxn` before the request is rerouted to the control group;
control placement never silently changes the transaction target.

## Resource settings

| Setting | Default | Meaning |
|---|---:|---|
| `EG_ANALYTICS_WORKERS` | `1` | Colocated executor concurrency (`0` = remote-only, maximum `32`) |
| `EG_ANALYTICS_TENANT_ACTIVE` | `2` | Maximum active leases per opaque tenant reference |
| `EG_ANALYTICS_TENANT_CPU_MS` | unlimited | Maximum reserved CPU budget per tenant |

Per-job policy carries CPU, memory, IO and output budgets, priority, deadline,
retry/backoff and placement constraints. Authenticated caller/purpose/tenant and
transaction item labels are pseudonymized before persistence.

## Verification

The source architecture gate is
`python scripts/check_p2_analytics_reasoning_architecture.py`; the current persisted
watermark shape is also covered by
`python scripts/check_persisted_mutation_contract.py`. Focused tests are
in the `eg-jobs` store/claim modules, association-mining cancellation tests, and
`eg-epistemic::incremental` tests. Suggested targeted commands are documented in
the implementation report; do not run multiple Rust builds concurrently on a
resource-constrained host.
