# Lane G5-h-txn: move the files placed in `medulla-h-txn`

Spec: `MEDULLA-CONTRACT-R056.8`. Start after: G4 landed.

## Base branch rule
Create the worktree from `main` after the previous group landed
(`git worktree add <wt> -b lane/crate-split-h-txn origin/main`). Never branch from another lane.

## Files to move (26; exactly these, nothing else)
- `src/server/handlers/dist_compute.rs`
- `src/server/handlers/sqlite_file.rs`
- `src/server/handlers/sqlite_file/transfer_fs.rs`
- `src/server/handlers/timeseries.rs`
- `src/server/handlers/timeseries/derived.rs`
- `src/server/handlers/timeseries/derived_tests.rs`
- `src/server/handlers/txn.rs`
- `src/server/handlers/txn/commit_core.rs`
- `src/server/handlers/txn/commit_modal.rs`
- `src/server/handlers/txn/commit_multi_graph.rs`
- `src/server/handlers/txn/commit_prepared.rs`
- `src/server/handlers/txn/consensus.rs`
- `src/server/handlers/txn/consensus/authority.rs`
- `src/server/handlers/txn/dispatch.rs`
- `src/server/handlers/txn/receipts.rs`
- `src/server/handlers/txn/receipts/lifecycle.rs`
- `src/server/handlers/txn/reconcile.rs`
- `src/server/handlers/txn/rollback.rs`
- `src/server/handlers/txn/staging.rs`
- `src/server/matview/mod.rs`
- `src/server/txn.rs`
- `src/server/txn/modality.rs`
- `src/server/txn/recovery_plan.rs`
- `src/server/txn/recovery_plan_tests.rs`
- `src/server/txn/replay_intent.rs`
- `src/server/txn_intent.rs`


## Codemod (run in this order, from the worktree root)
1. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-h-txn --moves > $SCRATCH/moves.sh`
2. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-h-txn --mods > $SCRATCH/mods.txt`
3. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-h-txn` rewrites `crate::<old path>` in the
   moved files and every caller, and points `placement.toml` at the new paths. Exit 1 prints the
   BLOCKED items and writes nothing.
4. `sh $SCRATCH/moves.sh` runs the `git mv` commands.
5. Apply `$SCRATCH/mods.txt`: add the `pub mod` lines to the new crate root and delete the listed
   `mod` declarations from the old parents. Move the crate's feature forwarding into its own
   `Cargo.toml` (the scaffold created the empty crate).
6. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-h-txn --check` must report nothing.

## Seams this lane uses (from SEAMS.md)
| category | item | refs | first location |
|---|---|---|---|
| extract-helper | `crate::server::dispatch::authoritative_now_ms` | 4 | src/server/handlers/sqlite_file.rs:197 |
| extract-type | `crate::server::handlers::admin::AdminSaga` | 11 | src/server/handlers/dist_compute.rs:284 |
| extract-type | `crate::server::handlers::admin::AdminSagaPayload` | 2 | src/server/handlers/txn/receipts.rs:393 |
| needs-seam | `crate::MeasurementBatch` | 4 | src/server/handlers/txn/commit_modal.rs:323 |
| needs-seam | `crate::server::dispatch::is_replicated_apply` | 2 | src/server/handlers/txn.rs:73 |
| needs-seam | `crate::server::sql_catalog_acl::require_source_authority` | 2 | src/server/handlers/sqlite_file.rs:267 |
| needs-seam | `crate::server::sql_catalog_acl::SqlSourceAuthorityWrite` | 1 | src/server/handlers/sqlite_file.rs:361 |
| needs-seam | `crate::server::sql_catalog_acl::register_owner_after_create_in` | 1 | src/server/handlers/sqlite_file.rs:366 |
| needs-seam | `crate::server::sql_catalog_acl::stable_source_operation_id` | 1 | src/server/handlers/sqlite_file.rs:369 |
| needs-seam | `crate::server::sql_catalog_acl::with_source_authority_write` | 1 | src/server/handlers/sqlite_file.rs:268 |
| seam:AdminApi | `crate::server::handlers::admin::resume_named_admin_saga` | 4 | src/server/handlers/txn/consensus/authority.rs:11 |
| seam:AdminApi | `crate::server::handlers::admin::begin_named_admin_saga_with_private_payload_and_nonce` | 2 | src/server/handlers/txn/receipts.rs:388 |
| seam:AdminApi | `crate::server::handlers::admin::finish_admin_saga` | 2 | src/server/handlers/txn/consensus/authority.rs:221 |
| seam:AdminApi | `crate::server::handlers::admin::begin_admin_saga` | 1 | src/server/handlers/dist_compute.rs:302 |
| seam:AdminApi | `crate::server::handlers::admin::begin_named_admin_saga_with_nonce` | 1 | src/server/handlers/txn/receipts/lifecycle.rs:171 |
| seam:AdminApi | `crate::server::handlers::admin::current_admin_saga_authority` | 1 | src/server/handlers/txn/receipts.rs:168 |
| seam:AdminApi | `crate::server::handlers::admin::decode_durable_saga_result` | 1 | src/server/handlers/txn.rs:102 |
| seam:QueryApi | `crate::server::handlers::query::overlay_write_set` | 1 | src/server/handlers/txn/staging.rs:578 |

Use the server-api trait or extracted item named in the table. Do not invent another seam.

## Files it must not touch
Every file outside the list above, except caller rewrites from step 3. Do not change
`scripts/crate_split/*.toml` by hand; step 3 updates `placement.toml`. Do not copy helpers:
move them. Do not add re-exports. Widen `pub(crate)` to `pub` only where placement requires it.

## Gates (no local cargo)
- `python3 scripts/check_crate_graph.py` passes.
- `python3 scripts/crate_split/edges.py` shows no back-edge out of `medulla-h-txn` except the listed seams.
- `python3 scripts/check_spec_paths.py` passes; rewrite the `Spec:` test bindings and spec file paths
  that name moved files in the same commit, then regenerate spec status.
- `ruff check` and `ruff format --check` on touched Python.

## Land
```
eg-lane-run enqueue --lane land-h-txn --worktree <wt> --test '-p medulla-h-txn::all()'
```
The change lands on a passing build-train run of its changed crates. Link the train result in the
pull request. Commit trailer: `Spec: MEDULLA-CONTRACT-R056.8`.

## STOP conditions
- Any `BLOCKED` or `needs-seam` hit (from `rewrite_paths.py` or the seam table below) means STOP. Write `BLOCKED.md` in the lane directory (outside the repository) naming the item and the file:line, then exit. Do not improvise a seam.
- Any file outside the list above, other than caller rewrites made by `rewrite_paths.py`, means STOP.
- Cold pull-request CI timing out is not a blocker (MEDULLA-CONTRACT-R056.12).
- Never run cargo locally, never `git stash`, never `git add -A` or `git add .`, never `--no-verify`, never force-push.
