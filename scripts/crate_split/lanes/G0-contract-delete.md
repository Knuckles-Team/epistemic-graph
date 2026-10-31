# Lane G0-contract-delete

Spec: `MEDULLA-CONTRACT-R056.1`. Start after: none; first lane.

## Base branch rule
Create the worktree from `main` after the previous group landed
(`git worktree add <wt> -b lane/crate-split-contract-delete origin/main`).

## Task
Delete `src/server/contract_wave.rs` and every reference to it (module declaration in `src/server/mod.rs`, uses in `dispatch/router/service_control.rs`, `handlers/admin/connector_pack/import/skill_files.rs`, `handlers/admin/connector_pack/import/modality_tests.rs`, `handlers/decide/shape.rs`, `handlers/work_item/lease_kind_policy.rs`). Do NOT delete `persistence/contract.rs`: it holds the `PersistenceBackend` trait and moves to server-api. Set `[delete] enforce = true` in `scripts/crate_split/manifest.toml` and remove the file's key from `placement.toml`.

## Files to touch
Only the files named in the task and in `scripts/crate_split/SEAMS.md` for this lane. Seam table:
see `scripts/crate_split/SEAMS.md`, sections `seam:*`, `extract-type`, `extract-helper`.

## Gates (no local cargo)
- `python3 scripts/check_crate_graph.py` passes.
- `python3 scripts/crate_split/edges.py` shows only listed seams.
- `python3 scripts/check_spec_paths.py` passes.

## Land
```
eg-lane-run enqueue --lane land-contract-delete --worktree <wt> --test '-p medulla-engine::all()'
```
Link the train result in the pull request. Commit trailer: `Spec: MEDULLA-CONTRACT-R056.1`.

## STOP conditions
- Any `BLOCKED` or `decide` hit (from `rewrite_paths.py` or the seam table below) means STOP. Write `BLOCKED.md` in the lane directory (outside the repository) naming the item and the file:line, then exit. Do not improvise a seam.
- Any file outside the list above, other than caller rewrites made by `rewrite_paths.py`, means STOP.
- Cold pull-request CI timing out is not a blocker (MEDULLA-CONTRACT-R056.12).
- Never run cargo locally, never `git stash`, never `git add -A` or `git add .`, never `--no-verify`, never force-push.
