# EG-SCM plan

## Architecture

A repository's source has exactly two homes in the engine. Both are existing surfaces.

1. **Blob store** — the existing content-addressed chunk store (`src/server/blob/store.rs`). Each
   file version stores once, keyed by its SHA-256 digest. The bounded streaming facade
   (`src/server/blob/stream.rs`) moves the bytes without buffering the whole file.
2. **Changeset chain** — the append-only branch chain of EG-BRANCH-MERGE. Each changeset names
   its target branch, its parents, and the paths it changed against the first parent. The chain
   is the source of truth for the repository files.

Everything else derives from the chain. Trees answer by replaying changesets from a snapshot
checkpoint. History indexes update incrementally. The code graph re-indexes the structurally
changed files. Gate verdicts and bisect decisions record against changeset ids.

## Reuse (inventory before new components)

| Need | Existing owner |
|---|---|
| Content-addressed store, dedup, refcount GC, S3 backend | blob CAS (`src/server/blob/store.rs`) |
| Bounded streaming of a blob to and from the CAS | streaming facade (`src/server/blob/stream.rs`) |
| Sealed create-only record guard | sealed record classes (EG-DURABLE-KERNEL-R057) |
| Append-only branch chain, commit-before-ack | branch commit path (EG-BRANCH-MERGE-R002) |
| File and line granularity three-way merge | merge pass (EG-BRANCH-MERGE-R004) |
| Version-checked concurrent writes, typed conflict | write conflict path (EG-DURABLE-KERNEL-R084) |
| Incremental derived series with lineage | derived series (EG-REPO-INGEST-R004) |
| Commit and change-coupling graph | history enrichment (EG-REPO-INGEST-R601) |
| Structural fingerprints, symbols, tests, Leiden communities | enrichment and community detection (EG-REPO-INGEST-R600/R603/R011) |
| Incremental indexing boundary with base-match refusal | incremental indexing (EG-REPO-INGEST-R006.4) |
| Universal read-RLS gate | read-RLS gate (EG-DURABLE-KERNEL-R029) |
| Typed verdict model | step verdict (EG-DURABLE-KERNEL-R089.1) |
| Replayable decision records | decision replay (EG-DECISION-ENGINE-R007) |
| Off-lock snapshot reads | snapshot path (EG-DURABLE-KERNEL-R023) |

No new store. No new consensus layer. No second writer authority.

## Delivery classes

| Row | Class |
|---|---|
| `EG-SCM-R001` | compose |
| `EG-SCM-R002` | build |
| `EG-SCM-R003` | compose |
| `EG-SCM-R004` | build |
| `EG-SCM-R005` | compose |
| `EG-SCM-R006` | wire |
| `EG-SCM-R007` | compose |
| `EG-SCM-R008` | compose |
| `EG-SCM-R009` | compose |
| `EG-SCM-R010` | build |
| `EG-SCM-R011` | build |
| `EG-SCM-R012` | wire |

## Interfaces

Wire surface at the request boundary. Generated clients follow.

- `SubmitChangeset { tenant, repo, branch, parents, author_principal, message, changed_paths,
  run_trace_id? }` returns an accept, a rebase receipt, or the typed conflict.
- `SourceTree { tenant, repo, rev, prefix?, depth? }` returns `{path, blob_digest, mode}` entries.
- `SourceBlob { tenant, repo, digest }` streams the bytes through the existing blob cursors.
- `SourceHistory { tenant, repo, path?, since? }` returns the changeset chain over the scope.
- `SourceBlame { tenant, repo, rev, path }` returns the line-to-changeset map.
- `RecordGateVerdict { tenant, repo, changeset, gate_id, input_digest, verdict,
  evidence_digest, duration }` returns the sealed verdict record.
- `BisectDecision { tenant, repo, failing_gate, from }` returns the culprit changeset and the
  recorded decision.

## Live wiring

Accept dispatches through the existing request boundary. Tenant RLS and principal resolution
apply before any read or write. The changeset append reuses the branch commit path. Its
durability is commit-before-ack through the existing raft path. The served `source.*` operations
and the git bridge belong to their owners. They consume these records through the wire surface
above.

## Decisions

- **Changesets are the source of truth, not git objects.** The engine stores file bytes by
  digest and file changes as chain records. Git objects appear at the export boundary only.
- **One chain per repository branch.** The changeset chain is the branch commit chain of
  EG-BRANCH-MERGE. No parallel log.
- **Tree reads replay from checkpoints.** Full history replay stays available. The checkpoint
  bounds the normal cost.
- **Conflicts are typed, never silent.** Disjoint paths land in parallel. Overlapping paths
  rebase on the server or refuse with `SOURCE_CONFLICT{paths}`.
- **Provenance is mandatory for agent principals.** An agent changeset names its run trace. A
  human changeset may omit it.

## Quality gates

Standard per-repo gates apply. Clippy, test, jscpd, CCCC, dupehound, KISS. The changeset record
and the tree query are new code. They pass the no-duplicate-authority check against the existing
commit and blob modules. The simulation harness runs in the spec gate. Release proof follows
EG-CONTRACT.


## Graph-native edits (R013) and structured trailers (R002.1)

An agent edits a code symbol, not a file. The engine owns the projection from symbol to bytes.

```text
agent -> arbora source.edit_symbol (ARBORA-OPS-R848.4)
      -> EG SubmitSymbolEdit
         1. resolve symbol_id at base_rev -> (path, blob_digest, byte span)   code graph, EG-REPO-INGEST-R600
         2. check anchor_span_digest == digest(span bytes)                     else EDIT_BASE_STALE
         3. splice ops per file, descending span order                         bytes outside spans unchanged
         4. parse_file (crates/eg-compute/src/parser/tree_sitter.rs:249)       re-parse each changed file
         5. compare symbols: edited resolves, untouched keep structure hash    else EDIT_ROUNDTRIP_MISMATCH
         6. write blobs (R001), build one changeset (R002), accept (R003)
         7. re-index (R006), derive lifecycle edges (EG-SDLC-R001)
      <- accept, server rebase, or typed refusal
```

The file projection is the authority for bytes. The code graph is the authority for structure.
The re-parse ties both: a changeset lands only when the new bytes parse back to the declared
symbol change. A plain file changeset through `SubmitChangeset` stays available for files with
no parser.

Trailers become typed fields on the changeset. The git bridge of R010 maps them to and from the
commit message trailers. EG-SDLC reads the typed fields, never the message text.
