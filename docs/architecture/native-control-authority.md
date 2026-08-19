# Native capacity and WorkItem admission authority

This document describes the additive native protocol implemented by the
epistemic-graph engine for the GOC-21 capacity lease and GOC-19 WorkItem submit
surfaces.  The engine's redb tables are authoritative; AU schedulers and local
queues are projections and must reconcile by the returned lease fences or
WorkItem command result.

## Capacity operations

All capacity DTOs use `schema_version: "1"`.  Every operation is scoped by the
request graph and tenant.  The server binds mutating timestamps to its
authoritative clock and, for external callers, requires `owner_digest` to equal
the verified principal persistence id.

* `AcquireCapacity { request }`: `tenant_ref`, `work_item_id`, `owner_digest`,
  `idempotency_key`, `priority`, `demands[]`, optional single-cell `lease_id`,
  `ttl_ms`, `now_ms`, and optional integer `cost_budget_micros`/`token_budget`.
  Each demand is `{cell_id, resource_class, amount}`.  The writer reclaims a
  bounded expiry page, checks every requested cell and aggregate usage, then
  inserts every lease and usage row in one immediate transaction.  The result
  is `{decision, leases[], available[], message}`; a denial is explicit
  (`exhausted`, `stale_epoch`, `stale_fence`, `expired`, `invalid`, or
  `backpressure`) and never partially charges a dimension.
* `RenewCapacity { request }` and `ReleaseCapacity { request }`: the request
  carries `tenant_ref`, `owner_digest`, `leases[]` of
  `{lease_id, lease_epoch, fence_token}`, `now_ms`, optional renewal `ttl_ms`,
  and optional `idempotency_key`.  All fences are prevalidated before any row
  is changed, so a batch is all-or-nothing.  Renew advances expiry and the
  renewal count; release decrements aggregate usage and terminalizes each row.
* `ReclaimExpiredCapacity { request }`: `{tenant_ref, cell_id?, max_count,
  now_ms, cursor?}`.  It atomically reclaims at most 128 expired rows for the
  tenant/cell selector and returns `{decision, reclaimed_lease_ids[],
  next_cursor}`.
* `ReconcileCapacity { request }` and `CapacityStatus { request }`: bounded
  `{tenant_ref, cell_id?, lease_id?, max_count, cursor?}` read pages.  Results
  contain native cells, tenant-visible leases, and `next_cursor`; no local
  mirror is consulted.
* `UpdateCapacityCell { request }`: controller-only `{cell,
  expected_epoch?, now_ms}`.  Cell epoch is monotonic, the optional expected
  epoch is a CAS guard, and capacity cannot be lowered below durable usage.

Capacity bounds are 16 acquire dimensions, 16 renew/release fences, 128 reclaim
or status rows, 512-byte opaque identifiers, amount ≤ 1,000,000,000, TTL ≤ 24
hours, and integer budget ≤ 1,000,000,000,000.  Fences are monotonic per cell;
renew/release reject an owner, epoch, or fence mismatch and never resurrect an
expired lease.  Acquire replay is keyed by `(graph, tenant_ref,
idempotency_key)` and compares a canonical request digest; key reuse with a
different request is `IDEMPOTENCY_CONFLICT`.

## WorkItem admission operations

`SubmitWorkItem { request }` uses the generated v2 `RequestContext` plus:

```text
work_item_id?, idempotency_key, command_digest, kind, priority,
depends_on[], input_ref, policy_digest, catalog_digest, model_digest,
max_attempts, deadline_unix?, metadata, provenance_refs[],
max_tenant_in_flight
```

The engine binds context tenant/graph/agent/audience/policy/scopes to the
verified carrier.  It validates the SHA-256 command digest, dependency
existence and same-tenant ownership, metadata/provenance/reference bounds, and
the tenant in-flight cap before allocating a graph-scoped monotonic command
sequence.  WorkItem node, dependency edges, command sequence, mutation-batch
status/idempotency, result, and transactional outbox are committed together.
The result explicitly returns `work_item_id`, `status`, `created`, `replayed`,
`command_sequence`, `idempotency_key`, dependency/quota snapshot,
`outbox_id`, command digest, provenance refs, and changed ids.

`SubmitWorkItems { request }` adds a parent `idempotency_key` and
`requests[]`.  It accepts 1–128 children, requires all child contexts to share
the parent tenant/graph, and applies the entire batch in one transaction.  A
replay returns the original ids and command sequences with `replayed: true`;
it never allocates another sequence or WorkItem.

Submit bounds are payload/reference ≤ 1 MiB, metadata ≤ 64 KiB, dependencies ≤
1024, provenance refs ≤ 64, priority in [-1024, 1024], attempts ≤ 4096, tenant
in-flight cap ≤ 4096, and the admission scan is capped at 50,000 graph rows.
Exceeding a bound or an admission/dependency condition is an explicit error; no
partial graph rows are acknowledged.

## Transport and AU integration points

The Rust protocol variants are `AcquireCapacity`, `RenewCapacity`,
`ReleaseCapacity`, `ReclaimExpiredCapacity`, `ReconcileCapacity`,
`CapacityStatus`, `UpdateCapacityCell`, `SubmitWorkItem`, and
`SubmitWorkItems`.  The async/sync Python client exposes them under
`client.capacity_leases` and `client.work_items.submit`/
`submit_batch`, with additive capability negotiation and fail-closed behavior
when an older engine does not advertise the method.

AU callers remain outside this engine lane.  The eventual adapter should replace
the read-before-create path in
`agent_utilities/orchestration/work_item.py`/`work_item_command.py` with
`client.work_items.submit`, and replace scheduler-local capacity gates in
`resource_priority.py`, `worker_scheduler.py`, `gpu_group_budget.py`,
`engine_tasks.py`, and the dispatch worker with
`client.capacity_leases.acquire`/`renew`/`release`/`reclaim`/`reconcile`.
It must pass the verified carrier-derived owner digest and preserve each native
`lease_epoch`/`fence_token`; local semaphores remain advisory only.
