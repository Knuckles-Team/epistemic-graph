# Lane G0-scaffold

Spec: `MEDULLA-CONTRACT-R056.11`. Start after: none; lands in the same group as the contract delete.

## Base branch rule
Create the worktree from `main` after the previous group landed
(`git worktree add <wt> -b lane/crate-split-scaffold origin/main`).

## Task
Create every empty crate listed in `scripts/crate_split/manifest.toml` with its own `Cargo.toml` (layering from the manifest, workspace member, feature forwarding stubs), an empty `src/lib.rs`, and facade re-export stubs in `medulla-engine`. One change, before any parallel lane. Dependencies follow the manifest levels only.

## Files to touch
Only the files named in the task and in `scripts/crate_split/SEAMS.md` for this lane. Seam table:
see `scripts/crate_split/SEAMS.md`, sections `seam:*`, `extract-type`, `extract-helper`.

## Gates (no local cargo)
- `python3 scripts/check_crate_graph.py` passes.
- `python3 scripts/crate_split/edges.py` shows only listed seams.
- `python3 scripts/check_spec_paths.py` passes.

## Land
```
eg-lane-run enqueue --lane land-scaffold --worktree <wt> --test '-p medulla-engine::all()'
```
Link the train result in the pull request. Commit trailer: `Spec: MEDULLA-CONTRACT-R056.11`.

## STOP conditions
- Any `BLOCKED` or `decide` hit (from `rewrite_paths.py` or the seam table below) means STOP. Write `BLOCKED.md` in the lane directory (outside the repository) naming the item and the file:line, then exit. Do not improvise a seam.
- Any file outside the list above, other than caller rewrites made by `rewrite_paths.py`, means STOP.
- Cold pull-request CI timing out is not a blocker (MEDULLA-CONTRACT-R056.12).
- Never run cargo locally, never `git stash`, never `git add -A` or `git add .`, never `--no-verify`, never force-push.
