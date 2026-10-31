# Lane G4-seam-traits

Spec: `MEDULLA-CONTRACT-R056.8`. Start after: G3 landed; single lane.

## Base branch rule
Create the worktree from `main` after the previous group landed
(`git worktree add <wt> -b lane/crate-split-seam-traits origin/main`).

## Task
In `medulla-server-api` add `HandleCtx::dispatch(Method)` re-entry, the `MethodHandler` and `HandlerRegistry` traits, the `DecideApi`, `QueryApi`, `AdminApi`, `TxnApi`, `JobsApi`, `IngestApi`, `GraphApi`, `ComputeApi` and `WireApi` traits named in SEAMS.md, the `seam:state-handle` trait-object handles, and the `extract-type` and `extract-helper` items. Convert every `seam:*` reference listed in SEAMS.md to the trait call. Behaviour must be identical, including handler-chain order.

## Files to touch
Only the files named in the task and in `scripts/crate_split/SEAMS.md` for this lane. Seam table:
see `scripts/crate_split/SEAMS.md`, sections `seam:*`, `extract-type`, `extract-helper`.

## Gates (no local cargo)
- `python3 scripts/check_crate_graph.py` passes.
- `python3 scripts/crate_split/edges.py` shows only listed seams.
- `python3 scripts/check_spec_paths.py` passes.

## Land
```
eg-lane-run enqueue --lane land-seam-traits --worktree <wt> --test '-p medulla-engine::all()'
```
Link the train result in the pull request. Commit trailer: `Spec: MEDULLA-CONTRACT-R056.8`.

## STOP conditions
- Any `BLOCKED` or `needs-seam` hit (from `rewrite_paths.py` or the seam table below) means STOP. Write `BLOCKED.md` in the lane directory (outside the repository) naming the item and the file:line, then exit. Do not improvise a seam.
- Any file outside the list above, other than caller rewrites made by `rewrite_paths.py`, means STOP.
- Cold pull-request CI timing out is not a blocker (MEDULLA-CONTRACT-R056.12).
- Never run cargo locally, never `git stash`, never `git add -A` or `git add .`, never `--no-verify`, never force-push.
