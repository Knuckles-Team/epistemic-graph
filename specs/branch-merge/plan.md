# EG-BRANCH-MERGE plan

## Architecture

Branch state has exactly two homes. Both are existing surfaces.

1. **Branch registration** — an owner-store lineage entry. It binds
   `(tenant, graph, name, base_engine_version, created_by, created_at)`. The owner-store layout
   lineage registry already records format and lineage state. It refuses unsupported layouts with
   typed errors (EG-DURABLE-KERNEL-R003). Branch registrations are new rows in that registry.
   They replicate with the rest of owner-store state.
2. **Branch commit chain** — append-only commits with parent pointers. A branch head is an engine
   version. `main` and every branch are named heads over the same commit DAG. A branch is a
   view. Creating it copies no rows.

The merge pass (R004/R005) computes over `(base_head, source_head, target_head)`. It classifies
each entity as unchanged, adopted, or conflicted. It validates through the existing constraint
and ABox authorities. It resolves epistemic conflicts. It publishes one atomic target change.
The publication uses the existing cross-modal transaction and raft path.

## Reuse (inventory before new components)

| Need | Existing owner |
|---|---|
| Durable registry rows + typed layout refusal | owner-store layout lineage (EG-DURABLE-KERNEL-R003) |
| Commit-before-ack durability, raft replication | existing commit/raft path; command sealing (EG-DURABLE-KERNEL-R009) |
| Idempotency, tenant-scoped | tenant-scoped lifecycle idempotency (EG-DURABLE-KERNEL-R019) |
| Edge identity | `(source, target, relation_type)` triple (EG-REPO-INGEST-R009.1) |
| Constraint / ABox validation on adopted entities | schema-repair validation authorities (EG-DECISION-ENGINE-R102.1) |
| Epistemic neighbourhood, source reliability | `WITH KNOWLEDGE` fields, source-reliability authority |
| Off-lock snapshot reads, tenant RLS | snapshot read path (EG-DURABLE-KERNEL-R023/R029) |
| Change-feed vocabulary | change-envelope records (EG-UNIFIED-DATA-PLANE-R008.1) |

No new store. No new consensus layer. No second writer authority.

## Interfaces

Wire surface at the request boundary. Generated clients follow.

- `CreateBranch { tenant, graph, name, base_engine_version? }` returns the branch identity.
- `Mutation` gains an optional `branch` target. The default is `main`. The commit receipt gains
  the branch head it advanced.
- `BranchMerge { tenant, graph, source, target, merge_base? }` returns the closed outcome
  (`up_to_date` / `fast_forward` / `merged` / `conflict`). It returns the two-parent commit
  receipt.
- `BranchDiff { tenant, graph, from, to }` returns the typed change feed against a baseline.
- `BranchReadAt { tenant, graph, branch, engine_version }` returns the as-of snapshot.

## Live wiring

Dispatch attaches to the existing request boundary. Authorization precedes results. Tenant RLS
applies. Merge publication reuses the commit publication path. The raft command catalog gains no
parallel mutation authority. The merge is a cross-modal transaction. Its effects seal like any
other commit.

## Decisions

- **View semantics, not copy-on-branch.** Branch creation costs one registry row. It costs no
  graph data.
- **Entity identity reuses existing authorities.** No new identity scheme. A node uses
  `(tenant, type, key)`. An edge uses `(source, target, relation_type)`.
- **Conflicts are epistemic records, not resolution heuristics.** The engine never guesses. An
  owner-pinned policy decides. Or the conflict stays open and queryable.
- **Closed outcome vocabulary.** Clients and agents branch on the result. They never parse
  prose.

## Quality gates

Standard per-repo gates apply. Clippy, test, jscpd, CCCC, dupehound, KISS. The merge pass is new
code. It passes the no-duplicate-authority check against the existing commit and
merge-adjacent modules. Release proof follows EG-CONTRACT.
