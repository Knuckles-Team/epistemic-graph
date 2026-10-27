# EG-BRANCH-MERGE — Agent Branching, Three-Way Merge, and Epistemic Conflict Resolution

Owner: Medulla (`EG`). Sole owner.

## User outcome

An agent or a person opens a named branch. The branch starts from the current engine state of a
graph. The agent writes to the branch in isolation. The branch merges back with a graph-wide
atomic merge. Every branch diffs against any other version. Every branch reads at any point in
its own commit history. A merge is entity-level three-way. A merge conflict is an epistemic
conflict. Both candidates keep their bitemporal windows. Both keep their provenance. Resolution
uses evidence weights under an owner-pinned policy. An unresolved conflict stays a typed,
queryable state. It is never a silent last-write-wins overwrite.

This gives long-horizon agents a reviewable write path. Each agent or task gets its own branch.
Merge happens on review. Time travel spans the whole graph. A git-style engine keeps
entity-level last-write-wins merges with constraint checks. The stated advantage is the merge
model: Medulla's merge is evidence-weighted and validity-aware. Its conflict receipts are
first-class epistemic records.

## Requirements

Every requirement ID this spec owns lives in [requirements.md](requirements.md). The IDs are
`EG-BRANCH-MERGE-R001`–`R007`. Architecture and live wiring live in [plan.md](plan.md). The
test contract lives in [test-spec.md](test-spec.md). Ordered work lives in
[tasks.md](tasks.md). Machine-readable delivery state lives in [status.json](status.json).

## Acceptance

The spec accepts when every row reaches `VERIFIED` in [status.json](status.json). Each row
carries its bound test evidence.

- A branch creates, writes in isolation, diffs, and reads at any of its versions (R001, R002,
  R006).
- A merge computes the right outcome (R003, R004, R007). The outcomes are up-to-date,
  fast-forward, three-way merged, or typed conflict. The target change publishes atomically
  across the whole graph. The receipt carries two parents.
- A conflicted entity resolves only through the epistemic policy (R005). Or it stays an open
  typed conflict. It keeps both candidates, their validity windows, and their evidence.

States and evidence follow the ecosystem spec standard. A planned or tested item is not a landed
item.
