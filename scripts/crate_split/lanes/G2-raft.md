# Lane G2-raft: move the files placed in `medulla-raft`

Spec: `MEDULLA-CONTRACT-R056.4`. Start after: G1 landed.

## Base branch rule
Create the worktree from `main` after the previous group landed
(`git worktree add <wt> -b lane/crate-split-raft origin/main`). Never branch from another lane.

## Files to move (64; exactly these, nothing else)
- `src/raft/command/mod.rs`
- `src/raft/command/native/catalog.rs`
- `src/raft/command/native/mod.rs`
- `src/raft/command/native/sealed.rs`
- `src/raft/command/native/tests.rs`
- `src/raft/cross_node_elasticity/mod.rs`
- `src/raft/cross_node_elasticity/plan.rs`
- `src/raft/cross_shard_txn/commit.rs`
- `src/raft/cross_shard_txn/mod.rs`
- `src/raft/cross_shard_txn/prepare_record.rs`
- `src/raft/drain.rs`
- `src/raft/exchange.rs`
- `src/raft/flake_budget.rs`
- `src/raft/harness/catchup_test.rs`
- `src/raft/harness/checker.rs`
- `src/raft/harness/cluster.rs`
- `src/raft/harness/cluster_slots.rs`
- `src/raft/harness/fixture.rs`
- `src/raft/harness/gauntlet_test.rs`
- `src/raft/harness/history.rs`
- `src/raft/harness/loadgen.rs`
- `src/raft/harness/mod.rs`
- `src/raft/harness/nemesis.rs`
- `src/raft/harness/resource_acceptance_test.rs`
- `src/raft/harness/trace_capture.rs`
- `src/raft/harness_support.rs`
- `src/raft/membership_shrink.rs`
- `src/raft/modality.rs`
- `src/raft/modality/tests.rs`
- `src/raft/multi.rs`
- `src/raft/multi/leader_balance.rs`
- `src/raft/multi/membership_follow.rs`
- `src/raft/multi/membership_removal.rs`
- `src/raft/multi/placement_commit.rs`
- `src/raft/network.rs`
- `src/raft/network/heartbeat_flush.rs`
- `src/raft/network/membership.rs`
- `src/raft/node.rs`
- `src/raft/placement.rs`
- `src/raft/placement/move_journal.rs`
- `src/raft/placement_harness.rs`
- `src/raft/pregel.rs`
- `src/raft/reshard.rs`
- `src/raft/reshard_harness.rs`
- `src/raft/store.rs`
- `src/raft/store/apply.rs`
- `src/raft/store/commit.rs`
- `src/raft/store/flush.rs`
- `src/raft/store/log.rs`
- `src/raft/store/native.rs`
- `src/raft/store/open.rs`
- `src/raft/store/replay.rs`
- `src/raft/store/snapshot.rs`
- `src/raft/store/state_machine.rs`
- `src/raft/xread.rs`
- `src/raft/xread/assemble.rs`
- `src/raft/xread/checks.rs`
- `src/raft/xread_harness.rs`
- `src/raft/xshard_harness.rs`
- `src/raft/xshard_modality_harness.rs`
- `src/server/handlers/modality.rs`
- `src/server/handlers/topology.rs`
- `src/server/icv_guard.rs`
- `src/server/matview/manager.rs`


## Codemod (run in this order, from the worktree root)
1. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-raft --moves > $SCRATCH/moves.sh`
2. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-raft --mods > $SCRATCH/mods.txt`
3. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-raft` rewrites `crate::<old path>` in the
   moved files and every caller, and points `placement.toml` at the new paths. Exit 1 prints the
   BLOCKED items and writes nothing.
4. `sh $SCRATCH/moves.sh` runs the `git mv` commands.
5. Apply `$SCRATCH/mods.txt`: add the `pub mod` lines to the new crate root and delete the listed
   `mod` declarations from the old parents. Move the crate's feature forwarding into its own
   `Cargo.toml` (the scaffold created the empty crate).
6. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-raft --check` must report nothing.

## Seams this lane uses (from SEAMS.md)
| category | item | refs | first location |
|---|---|---|---|
| needs-seam | `crate::server::txn::consensus_placement_fence_guard` | 7 | src/raft/multi.rs:1361 |
| needs-seam | `crate::server::txn::consensus_tenant_is_prepared` | 6 | src/raft/multi.rs:1362 |
| needs-seam | `crate::server::txn::consensus_graph_is_prepared` | 3 | src/raft/store/apply.rs:38 |
| needs-seam | `crate::server::ReplicatedParticipantRef` | 1 | src/raft/store/native.rs:156 |
| needs-seam | `crate::server::apply_replicated_job_publication_commit` | 1 | src/raft/store/native.rs:110 |
| needs-seam | `crate::server::apply_replicated_job_publication_finalize` | 1 | src/raft/store/native.rs:125 |
| needs-seam | `crate::server::apply_replicated_native` | 1 | src/raft/store/native.rs:377 |
| needs-seam | `crate::server::apply_replicated_transaction_decision` | 1 | src/raft/store/native.rs:83 |
| needs-seam | `crate::server::apply_replicated_transaction_finalize` | 1 | src/raft/store/native.rs:97 |
| needs-seam | `crate::server::apply_replicated_transaction_participant` | 1 | src/raft/store/native.rs:149 |
| needs-seam | `crate::server::apply_replicated_transaction_prepare` | 1 | src/raft/store/native.rs:366 |
| needs-seam | `crate::server::cdc::CdcHub::new` | 1 | src/raft/harness_support.rs:57 |
| needs-seam | `crate::server::cdc::emit_served_modality` | 1 | src/raft/store/commit.rs:381 |
| needs-seam | `crate::server::handlers::delegation::provenance_refs` | 1 | src/raft/harness/resource_acceptance_test.rs:2553 |
| needs-seam | `crate::server::handlers::txn::try_handle` | 1 | src/raft/xshard_harness.rs:553 |
| needs-seam | `crate::server::txn::consensus_control_conflicts` | 1 | src/raft/store/native.rs:348 |
| needs-seam | `crate::server::txn::consensus_has_prepared_graphs` | 1 | src/raft/multi.rs:1507 |
| seam:dispatch | `crate::server::dispatch` | 2 | src/raft/harness/resource_acceptance_test.rs:40 |

Use the server-api trait or extracted item named in the table. Do not invent another seam.

## Files it must not touch
Every file outside the list above, except caller rewrites from step 3. Do not change
`scripts/crate_split/*.toml` by hand; step 3 updates `placement.toml`. Do not copy helpers:
move them. Do not add re-exports. Widen `pub(crate)` to `pub` only where placement requires it.

## Gates (no local cargo)
- `python3 scripts/check_crate_graph.py` passes.
- `python3 scripts/crate_split/edges.py` shows no back-edge out of `medulla-raft` except the listed seams.
- `python3 scripts/check_spec_paths.py` passes; rewrite the `Spec:` test bindings and spec file paths
  that name moved files in the same commit, then regenerate spec status.
- `ruff check` and `ruff format --check` on touched Python.

## Land
```
eg-lane-run enqueue --lane land-raft --worktree <wt> --test '-p medulla-raft::all()'
```
The change lands on a passing build-train run of its changed crates. Link the train result in the
pull request. Commit trailer: `Spec: MEDULLA-CONTRACT-R056.4`.

## STOP conditions
- Any `BLOCKED` or `needs-seam` hit (from `rewrite_paths.py` or the seam table below) means STOP. Write `BLOCKED.md` in the lane directory (outside the repository) naming the item and the file:line, then exit. Do not improvise a seam.
- Any file outside the list above, other than caller rewrites made by `rewrite_paths.py`, means STOP.
- Cold pull-request CI timing out is not a blocker (MEDULLA-CONTRACT-R056.12).
- Never run cargo locally, never `git stash`, never `git add -A` or `git add .`, never `--no-verify`, never force-push.
