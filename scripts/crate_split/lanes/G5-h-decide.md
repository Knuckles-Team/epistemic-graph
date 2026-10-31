# Lane G5-h-decide: move the files placed in `medulla-h-decide`

Spec: `MEDULLA-CONTRACT-R056.8`. Start after: G4 landed.

## Base branch rule
Create the worktree from `main` after the previous group landed
(`git worktree add <wt> -b lane/crate-split-h-decide origin/main`). Never branch from another lane.

## Files to move (48; exactly these, nothing else)
- `src/server/handlers/decide.rs`
- `src/server/handlers/decide/assemble.rs`
- `src/server/handlers/decide/candidates.rs`
- `src/server/handlers/decide/capacity_premise.rs`
- `src/server/handlers/decide/commit.rs`
- `src/server/handlers/decide/commit/tests.rs`
- `src/server/handlers/decide/coverage.rs`
- `src/server/handlers/decide/coverage_tests.rs`
- `src/server/handlers/decide/guardrail.rs`
- `src/server/handlers/decide/jobs.rs`
- `src/server/handlers/decide/log.rs`
- `src/server/handlers/decide/served_adapter.rs`
- `src/server/handlers/decide/shape.rs`
- `src/server/handlers/decide/stat_adapter.rs`
- `src/server/handlers/decide/stat_belief.rs`
- `src/server/handlers/decide/stat_classes.rs`
- `src/server/handlers/decide/stat_decide.rs`
- `src/server/handlers/decide/stat_evaluator.rs`
- `src/server/handlers/decide/stat_executor.rs`
- `src/server/handlers/decide/stat_generation.rs`
- `src/server/handlers/decide/stat_harness_contrast.rs`
- `src/server/handlers/decide/stat_jobs.rs`
- `src/server/handlers/decide/stat_learning.rs`
- `src/server/handlers/decide/stat_log.rs`
- `src/server/handlers/decide/stat_nl.rs`
- `src/server/handlers/decide/stat_pointer.rs`
- `src/server/handlers/decide/stat_replay.rs`
- `src/server/handlers/decide/stat_reputation.rs`
- `src/server/handlers/decide/stat_resolve.rs`
- `src/server/handlers/decide/stat_retention.rs`
- `src/server/handlers/decide/stat_retrieval.rs`
- `src/server/handlers/decide/stat_retrieval_views.rs`
- `src/server/handlers/decide/stat_slate.rs`
- `src/server/handlers/decide/stat_slate_attribution.rs`
- `src/server/handlers/decide/stat_support.rs`
- `src/server/handlers/decide/stat_task_plan.rs`
- `src/server/handlers/decide/stat_tool_subset.rs`
- `src/server/handlers/decide/stat_vectors.rs`
- `src/server/handlers/decide/stat_view.rs`
- `src/server/handlers/decide/stat_walk_forward.rs`
- `src/server/handlers/decide/statistical.rs`
- `src/server/handlers/decide/telemetry.rs`
- `src/server/handlers/decide/topology_read.rs`
- `src/server/handlers/decide/topology_schema.rs`
- `src/server/handlers/decide/topology_shape.rs`
- `src/server/handlers/decide/topology_shape/tests.rs`
- `src/server/handlers/solve.rs`
- `src/server/sql_catalog_acl/relations.rs`


## Codemod (run in this order, from the worktree root)
1. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-h-decide --moves > $SCRATCH/moves.sh`
2. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-h-decide --mods > $SCRATCH/mods.txt`
3. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-h-decide` rewrites `crate::<old path>` in the
   moved files and every caller, and points `placement.toml` at the new paths. Exit 1 prints the
   BLOCKED items and writes nothing.
4. `sh $SCRATCH/moves.sh` runs the `git mv` commands.
5. Apply `$SCRATCH/mods.txt`: add the `pub mod` lines to the new crate root and delete the listed
   `mod` declarations from the old parents. Move the crate's feature forwarding into its own
   `Cargo.toml` (the scaffold created the empty crate).
6. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-h-decide --check` must report nothing.

## Seams this lane uses (from SEAMS.md)
| category | item | refs | first location |
|---|---|---|---|
| extract-helper | `crate::server::dispatch::authoritative_now_ms` | 6 | src/server/handlers/decide/assemble.rs:70 |
| extract-helper | `crate::server::dispatch::blocking_task` | 2 | src/server/handlers/decide/stat_jobs.rs:570 |
| extract-type | `crate::server::blob::BlobCursors` | 1 | src/server/handlers/decide/stat_retention.rs:32 |
| extract-type | `crate::server::blob::engine_bodies::EngineBody` | 1 | src/server/handlers/decide/stat_retention.rs:97 |
| extract-type | `crate::server::blob::store::HolderChange` | 1 | src/server/handlers/decide/stat_retention.rs:129 |
| extract-type | `crate::server::blob::store::HolderId` | 1 | src/server/handlers/decide/stat_retention.rs:129 |
| seam:AdminApi | `crate::server::handlers::admin::bind_agent_library_context` | 1 | src/server/handlers/decide/commit.rs:83 |
| seam:IngestApi | `crate::server::blob::engine_bodies::read_engine_body` | 1 | src/server/handlers/decide/stat_retention.rs:118 |
| seam:IngestApi | `crate::server::handlers::blob::compile_blob_batch` | 1 | src/server/handlers/decide/stat_retention.rs:137 |
| seam:QueryApi | `crate::server::handlers::query::uql_text_embedder` | 1 | src/server/handlers/decide/served_adapter.rs:198 |

Use the server-api trait or extracted item named in the table. Do not invent another seam.

## Files it must not touch
Every file outside the list above, except caller rewrites from step 3. Do not change
`scripts/crate_split/*.toml` by hand; step 3 updates `placement.toml`. Do not copy helpers:
move them. Do not add re-exports. Widen `pub(crate)` to `pub` only where placement requires it.

## Gates (no local cargo)
- `python3 scripts/check_crate_graph.py` passes.
- `python3 scripts/crate_split/edges.py` shows no back-edge out of `medulla-h-decide` except the listed seams.
- `python3 scripts/check_spec_paths.py` passes; rewrite the `Spec:` test bindings and spec file paths
  that name moved files in the same commit, then regenerate spec status.
- `ruff check` and `ruff format --check` on touched Python.

## Land
```
eg-lane-run enqueue --lane land-h-decide --worktree <wt> --test '-p medulla-h-decide::all()'
```
The change lands on a passing build-train run of its changed crates. Link the train result in the
pull request. Commit trailer: `Spec: MEDULLA-CONTRACT-R056.8`.

## STOP conditions
- Any `BLOCKED` or `needs-seam` hit (from `rewrite_paths.py` or the seam table below) means STOP. Write `BLOCKED.md` in the lane directory (outside the repository) naming the item and the file:line, then exit. Do not improvise a seam.
- Any file outside the list above, other than caller rewrites made by `rewrite_paths.py`, means STOP.
- Cold pull-request CI timing out is not a blocker (MEDULLA-CONTRACT-R056.12).
- Never run cargo locally, never `git stash`, never `git add -A` or `git add .`, never `--no-verify`, never force-push.
