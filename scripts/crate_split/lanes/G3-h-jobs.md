# Lane G3-h-jobs: move the files placed in `medulla-h-jobs`

Spec: `MEDULLA-CONTRACT-R056.7`. Start after: G2 landed.

## Base branch rule
Create the worktree from `main` after the previous group landed
(`git worktree add <wt> -b lane/crate-split-h-jobs origin/main`). Never branch from another lane.

## Files to move (35; exactly these, nothing else)
- `src/agent_lifecycle_statechart.rs`
- `src/loop_statechart.rs`
- `src/server/handlers/delegation.rs`
- `src/server/handlers/delegation/admission.rs`
- `src/server/handlers/delegation/validation.rs`
- `src/server/handlers/development_lane.rs`
- `src/server/handlers/jobs.rs`
- `src/server/handlers/jobs/association_hash_tests.rs`
- `src/server/handlers/jobs/core.rs`
- `src/server/handlers/jobs/executor_claim.rs`
- `src/server/handlers/jobs/executor_loop.rs`
- `src/server/handlers/jobs/executor_program.rs`
- `src/server/handlers/jobs/helpers.rs`
- `src/server/handlers/jobs/native_validation.rs`
- `src/server/handlers/jobs/prelude_jobs.rs`
- `src/server/handlers/jobs/prelude_server.rs`
- `src/server/handlers/jobs/prelude_std.rs`
- `src/server/handlers/jobs/program_validation.rs`
- `src/server/handlers/jobs/publication.rs`
- `src/server/handlers/jobs/results_association.rs`
- `src/server/handlers/jobs/results_program.rs`
- `src/server/handlers/jobs/submit.rs`
- `src/server/handlers/jobs/worker_publish.rs`
- `src/server/handlers/jobs/worker_requests.rs`
- `src/server/handlers/jobs/worker_validation.rs`
- `src/server/handlers/jobs_read_rls_architecture.rs`
- `src/server/handlers/mutation_outbox.rs`
- `src/server/handlers/native_write_authority.rs`
- `src/server/handlers/native_write_authority/tests.rs`
- `src/server/handlers/statechart.rs`
- `src/server/handlers/work_item.rs`
- `src/server/handlers/work_item/host_start_tests.rs`
- `src/server/handlers/work_item/lease_kind_policy.rs`
- `src/server/handlers/work_item_read.rs`
- `src/work_item_statechart.rs`


## Codemod (run in this order, from the worktree root)
1. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-h-jobs --moves > $SCRATCH/moves.sh`
2. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-h-jobs --mods > $SCRATCH/mods.txt`
3. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-h-jobs` rewrites `crate::<old path>` in the
   moved files and every caller, and points `placement.toml` at the new paths. Exit 1 prints the
   BLOCKED items and writes nothing.
4. `sh $SCRATCH/moves.sh` runs the `git mv` commands.
5. Apply `$SCRATCH/mods.txt`: add the `pub mod` lines to the new crate root and delete the listed
   `mod` declarations from the old parents. Move the crate's feature forwarding into its own
   `Cargo.toml` (the scaffold created the empty crate).
6. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-h-jobs --check` must report nothing.

## Seams this lane uses (from SEAMS.md)
| category | item | refs | first location |
|---|---|---|---|
| extract-helper | `crate::server::dispatch::authoritative_now_ms` | 10 | src/server/handlers/development_lane.rs:75 |
| extract-helper | `crate::server::dispatch::blocking_task` | 8 | src/server/handlers/mutation_outbox.rs:303 |
| extract-helper | `crate::server::dispatch::is_replicated_apply` | 2 | src/server/handlers/delegation/admission.rs:234 |
| extract-helper | `crate::server::dispatch::propose_native_mutation` | 1 | src/server/handlers/delegation/admission.rs:235 |
| extract-helper | `crate::server::handlers::*` | 1 | src/server/handlers/jobs_read_rls_architecture.rs:38 |
| pending-delete | `crate::server::contract_wave::not_yet_served` | 1 | src/server/handlers/work_item/lease_kind_policy.rs:128 |
| seam:QueryApi | `crate::server::semantic_index::existing_semantic_service` | 1 | src/server/handlers/mutation_outbox.rs:234 |

Use the server-api trait or extracted item named in the table. Do not invent another seam.

## Files it must not touch
Every file outside the list above, except caller rewrites from step 3. Do not change
`scripts/crate_split/*.toml` by hand; step 3 updates `placement.toml`. Do not copy helpers:
move them. Do not add re-exports. Widen `pub(crate)` to `pub` only where placement requires it.

## Gates (no local cargo)
- `python3 scripts/check_crate_graph.py` passes.
- `python3 scripts/crate_split/edges.py` shows no back-edge out of `medulla-h-jobs` except the listed seams.
- `python3 scripts/check_spec_paths.py` passes; rewrite the `Spec:` test bindings and spec file paths
  that name moved files in the same commit, then regenerate spec status.
- `ruff check` and `ruff format --check` on touched Python.

## Land
```
eg-lane-run enqueue --lane land-h-jobs --worktree <wt> --test '-p medulla-h-jobs::all()'
```
The change lands on a passing build-train run of its changed crates. Link the train result in the
pull request. Commit trailer: `Spec: MEDULLA-CONTRACT-R056.7`.

## STOP conditions
- Any `BLOCKED` or `decide` hit (from `rewrite_paths.py` or the seam table below) means STOP. Write `BLOCKED.md` in the lane directory (outside the repository) naming the item and the file:line, then exit. Do not improvise a seam.
- Any file outside the list above, other than caller rewrites made by `rewrite_paths.py`, means STOP.
- Cold pull-request CI timing out is not a blocker (MEDULLA-CONTRACT-R056.12).
- Never run cargo locally, never `git stash`, never `git add -A` or `git add .`, never `--no-verify`, never force-push.
