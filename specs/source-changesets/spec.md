# EG-SCM — Source changesets as the source of truth

Owner: epistemic-graph (`EG`). Sole owner.
Cross-repo IDs: none. The served `source.*` operations, the client transport, the gate
split and the git mirror sync belong to their owners. They file their rows as this
capability is delivered.

## User outcome

The engine holds a repository's source as a log of changesets. Each changeset stores
only the paths changed against its first parent. File bytes live once in the
content-addressed blob store. Trees, history and line attribution derive from the
changeset log.

A coding agent reads a tree at any revision without a clone. It submits a changeset.
It receives an accept, a server rebase, or a typed conflict. Concurrent writers on
disjoint paths all land with no global lock. A subtree read at any revision costs
time in proportion to the subtree, never the history length. Gate verdicts record
against each changeset. A failed post-landing gate yields a culprit and a replayable
revert-or-fix-forward decision. A repository exports to git and imports back with
identical commit and tree object ids.

This gives agents a write path over source with no local clone and no push step.
Each repository moves to engine authority on its own wave. A git mirror stays in
sync. A rollback returns authority to the mirror.

## Requirements

Every requirement ID this spec owns lives in [requirements.md](requirements.md). The IDs
are `EG-SCM-R001`–`R012`. Architecture and live wiring live in [plan.md](plan.md).
The test contract lives in [test-spec.md](test-spec.md). Ordered work lives in
[tasks.md](tasks.md). Machine-readable delivery state lives in [status.json](status.json).

## Why a new spec

The search covered `git grep -n -iE "changeset|blame|bisect|git blob" -- specs`.
It read EG-BRANCH-MERGE, EG-REPO-INGEST, EG-DURABLE-KERNEL and EG-DECISION-ENGINE.
EG-BRANCH-MERGE owns entity branches and merges, not file changesets.
EG-REPO-INGEST indexes external revisions. It does not hold the source of truth.
No spec owns a file changeset log, trees by query, or per-repository source
authority. This spec owns those parts. Each linked part stays with its owner.

## Acceptance

The spec accepts when every row reaches `VERIFIED` in [status.json](status.json). Each row
carries its bound test evidence.

- A changeset costs storage in proportion to its changed paths, never the
  repository size (R001, R002, R003).
- A subtree read at any revision costs time in proportion to the subtree, never
  the history length (R004, R005).
- A repository exports to git and imports back with identical commit and tree
  object ids (R010).
- Concurrent writers on disjoint paths all land with no global lock (R003, R011).

States and evidence follow the ecosystem spec standard. A planned or tested item
is not a landed item.
