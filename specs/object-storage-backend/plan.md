# EG-OBJECT-STORAGE-BACKEND plan

## Architecture

One physical store layer. Two backends. `crates/eg-storage` already binds owner-store authority
per write transaction (EG-DURABLE-KERNEL-R021). It keeps a layout lineage registry with typed
upgrade refusal (EG-DURABLE-KERNEL-R003). The object-store backend is a new physical binding
under that same authority.

- **Segments** — immutable, content-addressed objects. Keys are SHA-256. Each writes exactly
  once.
- **Version manifest** — one object per engine version. It binds `(version, digest, tenant,
  graph, parent_version, segment refs)`. Publishing a version is a single conditional manifest
  swap. The swap is a CAS on the manifest object. Losing the CAS means another publisher won.
  This publisher refuses as stale.
- **Reads** — fetch the manifest. Then fetch exactly the named segments. The existing
  `GraphView` snapshot machinery consumes them. There is no copy-on-read. There is no compaction
  trigger.

Consensus, placement, resharding, and idempotency stay untouched. They sit above the store
layer. They key off engine versions. The object store serves those identically.

## Reuse (inventory before new components)

| Need | Existing owner |
|---|---|
| Layout lineage registry + typed upgrade refusal | owner-store layout lineage (EG-DURABLE-KERNEL-R003) |
| Per-transaction store-authority binding | physical binding (EG-DURABLE-KERNEL-R021) |
| Snapshot reads, tenant RLS | `GraphView` off-lock snapshot path (EG-DURABLE-KERNEL-R023/R029) |
| Commit/raft path, receipts | existing commit path; raft sealing (EG-DURABLE-KERNEL-R009) |
| Placement / resharding / cold-tenant offload | M3 catalog-driven resharding authorities |
| Benchmark + cost-model machinery | transport benchmarks, multi-shard scaling, cost model docs |

No new consensus. No new writer authority. No parallel layout registry.

## Interfaces

- `StorageBackendSpec` (wire type, `crates/eg-types`). It carries the variant, the secret-ref,
  the durability class, and the layout lineage id.
- `OpenGraph` gains the backend spec. The open receipt names the resolved layout.
- Segment and manifest objects are engine-internal. No new public wire verbs appear beyond the
  spec and the open receipt.

## Live wiring

The physical binding in `crates/eg-storage` dispatches on the spec variant at open time. Every
later read and commit reuses the bound backend for the transaction's duration. This reuses the
existing once-per-transaction validation pattern. The comparative cost harness attaches to the
existing benchmark entry points. It forms a new scenario family.

## Decisions

- **CAS manifest publish, not a second consensus layer.** Object-store conditional writes give
  the atomic publish. Raft remains the authority for anything cross-node.
- **Bounded re-fetch for cold open.** A cold node fetches the current version's closure. It
  skips full history. History stays reachable by version. The engine fetches it on demand.
- **The cost gate is a release gate, not a per-PR gate.** One bounded regression guard covers
  the fixed open cost. That failure mode motivated the comparator's RFC 0031.

## Quality gates

Standard per-repo gates apply to the new binding and harness. The no-duplicate-authority check
confirms the object-store binding shares the store-authority chokepoint. It never bypasses it.
