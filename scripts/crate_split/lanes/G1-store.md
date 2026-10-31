# Lane G1-store: move the files placed in `medulla-store`

Spec: `MEDULLA-CONTRACT-R056.2`. Start after: G0 landed.

## Base branch rule
Create the worktree from `main` after the previous group landed
(`git worktree add <wt> -b lane/crate-split-store origin/main`). Never branch from another lane.

## Files to move (129; exactly these, nothing else)
- `src/loop_statechart.rs`
- `src/redb_layout.rs`
- `src/redb_store.rs`
- `src/redb_store/audit.rs`
- `src/redb_store/capacity_lease.rs`
- `src/redb_store/capacity_lease/headroom.rs`
- `src/redb_store/capacity_lease/throttle.rs`
- `src/redb_store/capacity_lease/throttle/tests.rs`
- `src/redb_store/checkpoint.rs`
- `src/redb_store/commit_ops_phase_measurement.rs`
- `src/redb_store/commit_timing.rs`
- `src/redb_store/control.rs`
- `src/redb_store/crossmodal.rs`
- `src/redb_store/development_lane.rs`
- `src/redb_store/development_lane/cleanup.rs`
- `src/redb_store/development_lane/dispatch.rs`
- `src/redb_store/development_lane/finish.rs`
- `src/redb_store/development_lane/identity.rs`
- `src/redb_store/development_lane/invocation.rs`
- `src/redb_store/development_lane/lifecycle_shared.rs`
- `src/redb_store/development_lane/links.rs`
- `src/redb_store/development_lane/quota.rs`
- `src/redb_store/development_lane/renew_observe.rs`
- `src/redb_store/development_lane/reserve.rs`
- `src/redb_store/development_lane/reserve_support.rs`
- `src/redb_store/development_lane/results.rs`
- `src/redb_store/development_lane/rows.rs`
- `src/redb_store/development_lane/status.rs`
- `src/redb_store/development_lane/test_cleanup_a.rs`
- `src/redb_store/development_lane/test_cleanup_b.rs`
- `src/redb_store/development_lane/test_finish_a.rs`
- `src/redb_store/development_lane/test_finish_b.rs`
- `src/redb_store/development_lane/test_finish_c.rs`
- `src/redb_store/development_lane/test_fixture.rs`
- `src/redb_store/development_lane/test_helpers.rs`
- `src/redb_store/development_lane/test_lifecycle_a.rs`
- `src/redb_store/development_lane/test_lifecycle_b.rs`
- `src/redb_store/development_lane/test_links_current.rs`
- `src/redb_store/development_lane/test_observe.rs`
- `src/redb_store/development_lane/test_reserve_a.rs`
- `src/redb_store/development_lane/test_reserve_b.rs`
- `src/redb_store/development_lane/test_seed.rs`
- `src/redb_store/development_lane/test_status_a.rs`
- `src/redb_store/development_lane/test_status_b.rs`
- `src/redb_store/development_lane/test_status_c.rs`
- `src/redb_store/development_lane/tests.rs`
- `src/redb_store/development_lane/types.rs`
- `src/redb_store/development_lane/validation.rs`
- `src/redb_store/development_lane/work_item.rs`
- `src/redb_store/dump.rs`
- `src/redb_store/enrichment_budget.rs`
- `src/redb_store/enrichment_budget/reactivation.rs`
- `src/redb_store/operation_audit.rs`
- `src/redb_store/operation_audit_tests.rs`
- `src/redb_store/resource.rs`
- `src/redb_store/resource/codec.rs`
- `src/redb_store/resource/extension.rs`
- `src/redb_store/resource/fingerprint.rs`
- `src/redb_store/resource/query.rs`
- `src/redb_store/resource/status.rs`
- `src/redb_store/resource/validation.rs`
- `src/redb_store/resource/write_admission.rs`
- `src/redb_store/resource/write_admit.rs`
- `src/redb_store/resource/write_lifecycle.rs`
- `src/redb_store/resource_reservation_tests.rs`
- `src/redb_store/scrub.rs`
- `src/redb_store/scrub_tests.rs`
- `src/redb_store/sealed_rows.rs`
- `src/redb_store/shard.rs`
- `src/redb_store/shard/batch.rs`
- `src/redb_store/shard_control_tests.rs`
- `src/redb_store/store_batch.rs`
- `src/redb_store/store_cleanup.rs`
- `src/redb_store/store_control.rs`
- `src/redb_store/store_mutation.rs`
- `src/redb_store/store_mutation/change_envelope.rs`
- `src/redb_store/store_native.rs`
- `src/redb_store/store_prelude.rs`
- `src/redb_store/store_read.rs`
- `src/redb_store/store_retry.rs`
- `src/redb_store/store_rows.rs`
- `src/redb_store/store_state.rs`
- `src/redb_store/store_state_tables.rs`
- `src/redb_store/store_types.rs`
- `src/redb_store/unreadable.rs`
- `src/redb_store/work_item.rs`
- `src/redb_store/work_item/cancel.rs`
- `src/redb_store/work_item/checkpoint.rs`
- `src/redb_store/work_item/checkpoint/tests.rs`
- `src/redb_store/work_item/claim.rs`
- `src/redb_store/work_item/commit.rs`
- `src/redb_store/work_item/control_lease.rs`
- `src/redb_store/work_item/control_lease/host_start.rs`
- `src/redb_store/work_item/control_lease/host_start/tests.rs`
- `src/redb_store/work_item/control_lease/pending_call.rs`
- `src/redb_store/work_item/control_lease/plan_pair.rs`
- `src/redb_store/work_item/control_lease/tests.rs`
- `src/redb_store/work_item/control_lease/tests/plan_consumption.rs`
- `src/redb_store/work_item/conversation.rs`
- `src/redb_store/work_item/conversation/tests.rs`
- `src/redb_store/work_item/current_plan.rs`
- `src/redb_store/work_item/current_plan/tests.rs`
- `src/redb_store/work_item/current_plan/tests/attachment.rs`
- `src/redb_store/work_item/dispatch.rs`
- `src/redb_store/work_item/lease.rs`
- `src/redb_store/work_item/lease/tests.rs`
- `src/redb_store/work_item/market.rs`
- `src/redb_store/work_item/market/lifecycle.rs`
- `src/redb_store/work_item/market/read.rs`
- `src/redb_store/work_item/market/tests.rs`
- `src/redb_store/work_item/market/upsert.rs`
- `src/redb_store/work_item/policy_record.rs`
- `src/redb_store/work_item/policy_record/tests.rs`
- `src/redb_store/work_item/read.rs`
- `src/redb_store/work_item/read/terminal_event.rs`
- `src/redb_store/work_item/read/tests.rs`
- `src/redb_store/work_item/row_guard.rs`
- `src/redb_store/work_item/run_owner.rs`
- `src/redb_store/work_item/sealed_record.rs`
- `src/redb_store/work_item/sealed_record_tests.rs`
- `src/redb_store/work_item/session_usage.rs`
- `src/redb_store/work_item/submit.rs`
- `src/redb_store/work_item/test_shard.rs`
- `src/redb_store/work_item/test_shard/run_fixture.rs`
- `src/redb_store/work_item_capability.rs`
- `src/redb_store/work_item_capability/authority.rs`
- `src/redb_store/work_item_capability/lease.rs`
- `src/sidecar_store.rs`
- `src/work_item_statechart.rs`


## Codemod (run in this order, from the worktree root)
1. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-store --moves > $SCRATCH/moves.sh`
2. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-store --mods > $SCRATCH/mods.txt`
3. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-store` rewrites `crate::<old path>` in the
   moved files and every caller, and points `placement.toml` at the new paths. Exit 1 prints the
   BLOCKED items and writes nothing.
4. `sh $SCRATCH/moves.sh` runs the `git mv` commands.
5. Apply `$SCRATCH/mods.txt`: add the `pub mod` lines to the new crate root and delete the listed
   `mod` declarations from the old parents. Move the crate's feature forwarding into its own
   `Cargo.toml` (the scaffold created the empty crate).
6. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-store --check` must report nothing.

## Seams this lane uses (from SEAMS.md)
| category | item | refs | first location |
|---|---|---|---|
| extract-helper | `crate::server::persistence::online_reshard::export_graph_raw` | 1 | src/redb_store/shard.rs:713 |
| extract-type | `crate::MeasurementBatch` | 6 | src/redb_store/crossmodal.rs:177 |
| extract-type | `crate::raft::EnrichmentTopUpTransition` | 2 | src/redb_store/store_mutation.rs:280 |
| extract-type | `crate::server::persistence::shard_migrate::GraphShardPayloadTransfer` | 1 | src/redb_store/shard.rs:910 |

Use the server-api trait or extracted item named in the table. Do not invent another seam.

## Files it must not touch
Every file outside the list above, except caller rewrites from step 3. Do not change
`scripts/crate_split/*.toml` by hand; step 3 updates `placement.toml`. Do not copy helpers:
move them. Do not add re-exports. Widen `pub(crate)` to `pub` only where placement requires it.

## Gates (no local cargo)
- `python3 scripts/check_crate_graph.py` passes.
- `python3 scripts/crate_split/edges.py` shows no back-edge out of `medulla-store` except the listed seams.
- `python3 scripts/check_spec_paths.py` passes; rewrite the `Spec:` test bindings and spec file paths
  that name moved files in the same commit, then regenerate spec status.
- `ruff check` and `ruff format --check` on touched Python.

## Land
```
eg-lane-run enqueue --lane land-store --worktree <wt> --test '-p medulla-store::all()'
```
The change lands on a passing build-train run of its changed crates. Link the train result in the
pull request. Commit trailer: `Spec: MEDULLA-CONTRACT-R056.2`.

## STOP conditions
- Any `BLOCKED` or `decide` hit (from `rewrite_paths.py` or the seam table below) means STOP. Write `BLOCKED.md` in the lane directory (outside the repository) naming the item and the file:line, then exit. Do not improvise a seam.
- Any file outside the list above, other than caller rewrites made by `rewrite_paths.py`, means STOP.
- Cold pull-request CI timing out is not a blocker (MEDULLA-CONTRACT-R056.12).
- Never run cargo locally, never `git stash`, never `git add -A` or `git add .`, never `--no-verify`, never force-push.
