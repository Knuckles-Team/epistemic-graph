# Lane G2-persistence: move the files placed in `medulla-persistence`

Spec: `MEDULLA-CONTRACT-R056.6`. Start after: G1 landed.

## Base branch rule
Create the worktree from `main` after the previous group landed
(`git worktree add <wt> -b lane/crate-split-persistence origin/main`). Never branch from another lane.

## Files to move (105; exactly these, nothing else)
- `src/cost/mod.rs`
- `src/raft/config.rs`
- `src/server/cold_tier_impl.rs`
- `src/server/graph_schema/compose.rs`
- `src/server/persistence/agent_component.rs`
- `src/server/persistence/agent_component/pins.rs`
- `src/server/persistence/agent_component/write.rs`
- `src/server/persistence/agent_fixtures.rs`
- `src/server/persistence/agent_graph.rs`
- `src/server/persistence/agent_graph/references.rs`
- `src/server/persistence/agent_graph/tests.rs`
- `src/server/persistence/agent_graph/write.rs`
- `src/server/persistence/agent_library.rs`
- `src/server/persistence/agent_library/batch.rs`
- `src/server/persistence/agent_library/history.rs`
- `src/server/persistence/agent_library/open_tests.rs`
- `src/server/persistence/agent_library/receipt.rs`
- `src/server/persistence/agent_library/replay.rs`
- `src/server/persistence/agent_library/write.rs`
- `src/server/persistence/agent_pin_resolution.rs`
- `src/server/persistence/agent_revision.rs`
- `src/server/persistence/agent_revision/status.rs`
- `src/server/persistence/agent_revision/tests.rs`
- `src/server/persistence/agent_revision/write.rs`
- `src/server/persistence/agent_row.rs`
- `src/server/persistence/agent_template.rs`
- `src/server/persistence/agent_template/tests.rs`
- `src/server/persistence/agent_template/write.rs`
- `src/server/persistence/backup.rs`
- `src/server/persistence/cluster_hierarchy_store.rs`
- `src/server/persistence/cold_offload.rs`
- `src/server/persistence/connector_pack.rs`
- `src/server/persistence/connector_pack/admin.rs`
- `src/server/persistence/connector_pack/admitted.rs`
- `src/server/persistence/connector_pack/approval.rs`
- `src/server/persistence/connector_pack/catalog_authority.rs`
- `src/server/persistence/connector_pack/commit.rs`
- `src/server/persistence/connector_pack/declaration.rs`
- `src/server/persistence/connector_pack/outbox.rs`
- `src/server/persistence/connector_pack/pins.rs`
- `src/server/persistence/connector_pack/projection.rs`
- `src/server/persistence/connector_pack/visibility.rs`
- `src/server/persistence/decision_jobs.rs`
- `src/server/persistence/decision_record.rs`
- `src/server/persistence/decision_record/sources.rs`
- `src/server/persistence/decision_record/tests.rs`
- `src/server/persistence/durable_stores.rs`
- `src/server/persistence/fleet_components.rs`
- `src/server/persistence/guardrail_rule.rs`
- `src/server/persistence/guardrail_rule/tests.rs`
- `src/server/persistence/mod.rs`
- `src/server/persistence/node_info_store.rs`
- `src/server/persistence/online_reshard.rs`
- `src/server/persistence/online_reshard/diff.rs`
- `src/server/persistence/provenance_anchor.rs`
- `src/server/persistence/read_through.rs`
- `src/server/persistence/rebalance.rs`
- `src/server/persistence/redb_backend.rs`
- `src/server/persistence/redb_backend/audit_append_tests.rs`
- `src/server/persistence/redb_backend/backend_audit.rs`
- `src/server/persistence/redb_backend/backend_backup.rs`
- `src/server/persistence/redb_backend/backend_load.rs`
- `src/server/persistence/redb_backend/backend_open.rs`
- `src/server/persistence/redb_backend/backend_routing.rs`
- `src/server/persistence/redb_backend/backend_shard_routes.rs`
- `src/server/persistence/redb_backend/commands.rs`
- `src/server/persistence/redb_backend/matview_api.rs`
- `src/server/persistence/redb_backend/raft_api.rs`
- `src/server/persistence/redb_backend/raft_linger_tests.rs`
- `src/server/persistence/redb_backend/scrub_api.rs`
- `src/server/persistence/redb_backend/scrub_api_tests.rs`
- `src/server/persistence/redb_backend/shard_writer.rs`
- `src/server/persistence/redb_backend/timeseries.rs`
- `src/server/persistence/redb_backend/trait_capabilities.rs`
- `src/server/persistence/redb_backend/trait_envelopes.rs`
- `src/server/persistence/redb_backend/trait_graph.rs`
- `src/server/persistence/redb_backend/trait_graph_reads.rs`
- `src/server/persistence/redb_backend/trait_impl.rs`
- `src/server/persistence/redb_backend/trait_mutations.rs`
- `src/server/persistence/redb_backend/trait_native.rs`
- `src/server/persistence/redb_backend/trait_outbox.rs`
- `src/server/persistence/redb_backend/work_item_read.rs`
- `src/server/persistence/redb_backend/writer_commands.rs`
- `src/server/persistence/redb_backend/writer_thread.rs`
- `src/server/persistence/redb_backend/xshard_api.rs`
- `src/server/persistence/shard_migrate.rs`
- `src/server/persistence/shard_migrate/inplace.rs`
- `src/server/persistence/shard_migrate/payload.rs`
- `src/server/persistence/shard_migrate/readiness.rs`
- `src/server/persistence/shard_migrate/swap.rs`
- `src/server/persistence/shard_migrate/tests.rs`
- `src/server/persistence/shard_migrate/tests/layout.rs`
- `src/server/persistence/shard_migrate/tests/payload.rs`
- `src/server/persistence/shard_migrate/tests/recovery.rs`
- `src/server/persistence/storage_scrub.rs`
- `src/server/persistence/store_upgrade.rs`
- `src/server/persistence/store_upgrade/auto_migrate.rs`
- `src/server/persistence/store_upgrade/auto_migrate_tests.rs`
- `src/server/persistence/store_upgrade/discover.rs`
- `src/server/persistence/store_upgrade/report.rs`
- `src/server/persistence/store_upgrade/tests.rs`
- `src/server/persistence/tenant_catalog.rs`
- `src/server/persistence/write_back.rs`
- `src/server/persistence/writer_reply.rs`
- `src/server/sql_tables.rs`


## Codemod (run in this order, from the worktree root)
1. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-persistence --moves > $SCRATCH/moves.sh`
2. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-persistence --mods > $SCRATCH/mods.txt`
3. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-persistence` rewrites `crate::<old path>` in the
   moved files and every caller, and points `placement.toml` at the new paths. Exit 1 prints the
   BLOCKED items and writes nothing.
4. `sh $SCRATCH/moves.sh` runs the `git mv` commands.
5. Apply `$SCRATCH/mods.txt`: add the `pub mod` lines to the new crate root and delete the listed
   `mod` declarations from the old parents. Move the crate's feature forwarding into its own
   `Cargo.toml` (the scaffold created the empty crate).
6. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-persistence --check` must report nothing.

## Seams this lane uses (from SEAMS.md)
| category | item | refs | first location |
|---|---|---|---|
| extract-helper | `crate::server::dispatch::authoritative_now_ms` | 5 | src/server/persistence/agent_library/write.rs:243 |
| needs-seam | `crate::server::txn::now_ms` | 5 | src/server/persistence/online_reshard.rs:1262 |
| needs-seam | `crate::MeasurementBatch` | 4 | src/server/persistence/mod.rs:147 |
| needs-seam | `crate::server::kv::KV_PHYSICAL_STORE` | 1 | src/server/persistence/durable_stores.rs:184 |
| needs-seam | `crate::server::txn::GraphTxnState::decode_recovery_plan` | 1 | src/server/persistence/redb_backend.rs:672 |

Use the server-api trait or extracted item named in the table. Do not invent another seam.

## Files it must not touch
Every file outside the list above, except caller rewrites from step 3. Do not change
`scripts/crate_split/*.toml` by hand; step 3 updates `placement.toml`. Do not copy helpers:
move them. Do not add re-exports. Widen `pub(crate)` to `pub` only where placement requires it.

## Gates (no local cargo)
- `python3 scripts/check_crate_graph.py` passes.
- `python3 scripts/crate_split/edges.py` shows no back-edge out of `medulla-persistence` except the listed seams.
- `python3 scripts/check_spec_paths.py` passes; rewrite the `Spec:` test bindings and spec file paths
  that name moved files in the same commit, then regenerate spec status.
- `ruff check` and `ruff format --check` on touched Python.

## Land
```
eg-lane-run enqueue --lane land-persistence --worktree <wt> --test '-p medulla-persistence::all()'
```
The change lands on a passing build-train run of its changed crates. Link the train result in the
pull request. Commit trailer: `Spec: MEDULLA-CONTRACT-R056.6`.

## STOP conditions
- Any `BLOCKED` or `needs-seam` hit (from `rewrite_paths.py` or the seam table below) means STOP. Write `BLOCKED.md` in the lane directory (outside the repository) naming the item and the file:line, then exit. Do not improvise a seam.
- Any file outside the list above, other than caller rewrites made by `rewrite_paths.py`, means STOP.
- Cold pull-request CI timing out is not a blocker (MEDULLA-CONTRACT-R056.12).
- Never run cargo locally, never `git stash`, never `git add -A` or `git add .`, never `--no-verify`, never force-push.
