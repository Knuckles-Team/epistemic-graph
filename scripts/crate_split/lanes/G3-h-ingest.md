# Lane G3-h-ingest: move the files placed in `medulla-h-ingest`

Spec: `MEDULLA-CONTRACT-R056.7`. Start after: G2 landed.

## Base branch rule
Create the worktree from `main` after the previous group landed
(`git worktree add <wt> -b lane/crate-split-h-ingest origin/main`). Never branch from another lane.

## Files to move (40; exactly these, nothing else)
- `src/server/blob/cas_resolver.rs`
- `src/server/blob/cdc.rs`
- `src/server/blob/engine_bodies.rs`
- `src/server/blob/mod.rs`
- `src/server/blob/s3.rs`
- `src/server/blob/store.rs`
- `src/server/blob/store/gc.rs`
- `src/server/blob/store/holders.rs`
- `src/server/blob/store/killpoint.rs`
- `src/server/blob/store/manifest.rs`
- `src/server/blob/store/native.rs`
- `src/server/blob/store/native/chunks.rs`
- `src/server/blob/store/native/engine.rs`
- `src/server/blob/store/native/holders.rs`
- `src/server/blob/store/native/manifests.rs`
- `src/server/blob/store/native/uploads.rs`
- `src/server/blob/store/peak_rss.rs`
- `src/server/blob/store/policy.rs`
- `src/server/blob/store/sidecar.rs`
- `src/server/blob/store/test_support.rs`
- `src/server/blob/store/tests.rs`
- `src/server/blob/store/uploads.rs`
- `src/server/blob/stream.rs`
- `src/server/handlers/asr.rs`
- `src/server/handlers/blob.rs`
- `src/server/handlers/modality/dispatch.rs`
- `src/server/handlers/modality/ingest.rs`
- `src/server/handlers/modality/migration.rs`
- `src/server/handlers/modality/response.rs`
- `src/server/handlers/repository_index.rs`
- `src/server/handlers/source_batch.rs`
- `src/server/handlers/source_batch/authorization.rs`
- `src/server/handlers/source_batch/tests.rs`
- `src/server/handlers/source_ingestion.rs`
- `src/server/handlers/web_ingest.rs`
- `src/server/handlers/web_ingest/embedder.rs`
- `src/server/repository_enrichment_worker.rs`
- `src/server/repository_enrichment_worker/grant.rs`
- `src/server/repository_enrichment_worker/raft_park.rs`
- `src/server/repository_enrichment_worker/raft_top_up.rs`


## Codemod (run in this order, from the worktree root)
1. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-h-ingest --moves > $SCRATCH/moves.sh`
2. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-h-ingest --mods > $SCRATCH/mods.txt`
3. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-h-ingest` rewrites `crate::<old path>` in the
   moved files and every caller, and points `placement.toml` at the new paths. Exit 1 prints the
   BLOCKED items and writes nothing.
4. `sh $SCRATCH/moves.sh` runs the `git mv` commands.
5. Apply `$SCRATCH/mods.txt`: add the `pub mod` lines to the new crate root and delete the listed
   `mod` declarations from the old parents. Move the crate's feature forwarding into its own
   `Cargo.toml` (the scaffold created the empty crate).
6. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-h-ingest --check` must report nothing.

## Seams this lane uses (from SEAMS.md)
| category | item | refs | first location |
|---|---|---|---|
| extract-helper | `crate::server::dispatch::authoritative_now_ms` | 12 | src/server/blob/mod.rs:69 |
| extract-helper | `crate::server::dispatch::submit_repository_enrichment_page` | 1 | src/server/repository_enrichment_worker.rs:314 |
| extract-helper | `crate::server::outbound_http_config` | 1 | src/server/handlers/web_ingest/embedder.rs:114 |
| extract-helper | `crate::server::unique_temp_dir` | 1 | src/server/blob/store.rs:492 |
| needs-seam | `crate::server::dispatch::DrainOutcome::Acknowledged` | 1 | src/server/repository_enrichment_worker.rs:207 |
| needs-seam | `crate::server::dispatch::decode_pending_enrichment_intent` | 1 | src/server/repository_enrichment_worker/raft_top_up.rs:255 |
| needs-seam | `crate::server::dispatch::drain_repository_enrichment_once` | 1 | src/server/repository_enrichment_worker.rs:171 |
| needs-seam | `crate::server::dispatch::enrichment_intent_for_snapshot` | 1 | src/server/repository_enrichment_worker/raft_top_up.rs:272 |
| needs-seam | `crate::server::dispatch::plan_held_underfunded_park` | 1 | src/server/repository_enrichment_worker/raft_park.rs:91 |
| needs-seam | `crate::server::sql_catalog_acl::ACCESS_DENIED` | 1 | src/server/handlers/source_batch/authorization.rs:6 |
| needs-seam | `crate::server::sql_catalog_acl::SqlPrivilege` | 1 | src/server/handlers/source_batch/authorization.rs:6 |
| needs-seam | `crate::server::sql_catalog_acl::SqlSourceAuthorityWrite` | 1 | src/server/handlers/source_batch/authorization.rs:6 |
| needs-seam | `crate::server::sql_catalog_acl::with_source_authority_write` | 1 | src/server/handlers/source_batch.rs:108 |

Use the server-api trait or extracted item named in the table. Do not invent another seam.

## Files it must not touch
Every file outside the list above, except caller rewrites from step 3. Do not change
`scripts/crate_split/*.toml` by hand; step 3 updates `placement.toml`. Do not copy helpers:
move them. Do not add re-exports. Widen `pub(crate)` to `pub` only where placement requires it.

## Gates (no local cargo)
- `python3 scripts/check_crate_graph.py` passes.
- `python3 scripts/crate_split/edges.py` shows no back-edge out of `medulla-h-ingest` except the listed seams.
- `python3 scripts/check_spec_paths.py` passes; rewrite the `Spec:` test bindings and spec file paths
  that name moved files in the same commit, then regenerate spec status.
- `ruff check` and `ruff format --check` on touched Python.

## Land
```
eg-lane-run enqueue --lane land-h-ingest --worktree <wt> --test '-p medulla-h-ingest::all()'
```
The change lands on a passing build-train run of its changed crates. Link the train result in the
pull request. Commit trailer: `Spec: MEDULLA-CONTRACT-R056.7`.

## STOP conditions
- Any `BLOCKED` or `needs-seam` hit (from `rewrite_paths.py` or the seam table below) means STOP. Write `BLOCKED.md` in the lane directory (outside the repository) naming the item and the file:line, then exit. Do not improvise a seam.
- Any file outside the list above, other than caller rewrites made by `rewrite_paths.py`, means STOP.
- Cold pull-request CI timing out is not a blocker (MEDULLA-CONTRACT-R056.12).
- Never run cargo locally, never `git stash`, never `git add -A` or `git add .`, never `--no-verify`, never force-push.
