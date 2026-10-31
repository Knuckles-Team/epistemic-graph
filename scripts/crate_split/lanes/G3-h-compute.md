# Lane G3-h-compute: move the files placed in `medulla-h-compute`

Spec: `MEDULLA-CONTRACT-R056.7`. Start after: G2 landed.

## Base branch rule
Create the worktree from `main` after the previous group landed
(`git worktree add <wt> -b lane/crate-split-h-compute origin/main`). Never branch from another lane.

## Files to move (17; exactly these, nothing else)
- `src/server/graph_tile_server.rs`
- `src/server/graph_tile_source.rs`
- `src/server/handlers/datascience.rs`
- `src/server/handlers/datascience/routes.rs`
- `src/server/handlers/finance.rs`
- `src/server/handlers/finance/market.rs`
- `src/server/handlers/finance/numeric_kernels.rs`
- `src/server/handlers/finance/route_families.rs`
- `src/server/handlers/finance/routes.rs`
- `src/server/handlers/finance/signal_models.rs`
- `src/server/handlers/quantum.rs`
- `src/server/handlers/quantum/tests.rs`
- `src/server/handlers/viz.rs`
- `src/server/handlers/wasm_udf.rs`
- `src/server/viz_engine.rs`
- `src/server/viz_interactive.rs`
- `src/server/viz_provenance.rs`


## Codemod (run in this order, from the worktree root)
1. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-h-compute --moves > $SCRATCH/moves.sh`
2. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-h-compute --mods > $SCRATCH/mods.txt`
3. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-h-compute` rewrites `crate::<old path>` in the
   moved files and every caller, and points `placement.toml` at the new paths. Exit 1 prints the
   BLOCKED items and writes nothing.
4. `sh $SCRATCH/moves.sh` runs the `git mv` commands.
5. Apply `$SCRATCH/mods.txt`: add the `pub mod` lines to the new crate root and delete the listed
   `mod` declarations from the old parents. Move the crate's feature forwarding into its own
   `Cargo.toml` (the scaffold created the empty crate).
6. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-h-compute --check` must report nothing.

## Seams this lane uses (from SEAMS.md)
No seam, extraction or blocker touches this crate's files.

Use the server-api trait or extracted item named in the table. Do not invent another seam.

## Files it must not touch
Every file outside the list above, except caller rewrites from step 3. Do not change
`scripts/crate_split/*.toml` by hand; step 3 updates `placement.toml`. Do not copy helpers:
move them. Do not add re-exports. Widen `pub(crate)` to `pub` only where placement requires it.

## Gates (no local cargo)
- `python3 scripts/check_crate_graph.py` passes.
- `python3 scripts/crate_split/edges.py` shows no back-edge out of `medulla-h-compute` except the listed seams.
- `python3 scripts/check_spec_paths.py` passes; rewrite the `Spec:` test bindings and spec file paths
  that name moved files in the same commit, then regenerate spec status.
- `ruff check` and `ruff format --check` on touched Python.

## Land
```
eg-lane-run enqueue --lane land-h-compute --worktree <wt> --test '-p medulla-h-compute::all()'
```
The change lands on a passing build-train run of its changed crates. Link the train result in the
pull request. Commit trailer: `Spec: MEDULLA-CONTRACT-R056.7`.

## STOP conditions
- Any `BLOCKED` or `decide` hit (from `rewrite_paths.py` or the seam table below) means STOP. Write `BLOCKED.md` in the lane directory (outside the repository) naming the item and the file:line, then exit. Do not improvise a seam.
- Any file outside the list above, other than caller rewrites made by `rewrite_paths.py`, means STOP.
- Cold pull-request CI timing out is not a blocker (MEDULLA-CONTRACT-R056.12).
- Never run cargo locally, never `git stash`, never `git add -A` or `git add .`, never `--no-verify`, never force-push.
