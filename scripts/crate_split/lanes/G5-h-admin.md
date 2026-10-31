# Lane G5-h-admin: move the files placed in `medulla-h-admin`

Spec: `MEDULLA-CONTRACT-R056.8`. Start after: G4 landed.

## Base branch rule
Create the worktree from `main` after the previous group landed
(`git worktree add <wt> -b lane/crate-split-h-admin origin/main`). Never branch from another lane.

## Files to move (45; exactly these, nothing else)
- `src/server/connector_pack_projection.rs`
- `src/server/connector_pack_projection/boot_regression.rs`
- `src/server/connector_pack_projection/grant.rs`
- `src/server/graph_schema.rs`
- `src/server/graph_schema/approved.rs`
- `src/server/graph_schema/attach_pack.rs`
- `src/server/graph_schema/compiled.rs`
- `src/server/graph_schema/finance_tests.rs`
- `src/server/graph_schema/swarm_topology_tests.rs`
- `src/server/graph_schema/terms.rs`
- `src/server/graph_schema/terms_served_tests.rs`
- `src/server/graph_schema/test_support.rs`
- `src/server/graph_schema/tests.rs`
- `src/server/graph_schema/virtual_graph_tests.rs`
- `src/server/graph_schema/world_model_tests.rs`
- `src/server/handlers/admin.rs`
- `src/server/handlers/admin/agent.rs`
- `src/server/handlers/admin/backup.rs`
- `src/server/handlers/admin/cluster.rs`
- `src/server/handlers/admin/component_content.rs`
- `src/server/handlers/admin/connector_pack.rs`
- `src/server/handlers/admin/connector_pack/admin.rs`
- `src/server/handlers/admin/connector_pack/approval.rs`
- `src/server/handlers/admin/connector_pack/catalog_authority.rs`
- `src/server/handlers/admin/connector_pack/import.rs`
- `src/server/handlers/admin/connector_pack/import/annotations.rs`
- `src/server/handlers/admin/connector_pack/import/capability_tests.rs`
- `src/server/handlers/admin/connector_pack/import/catalog_attributes.rs`
- `src/server/handlers/admin/connector_pack/import/desired.rs`
- `src/server/handlers/admin/connector_pack/import/facts.rs`
- `src/server/handlers/admin/connector_pack/import/front_matter.rs`
- `src/server/handlers/admin/connector_pack/import/json.rs`
- `src/server/handlers/admin/connector_pack/import/modality_tests.rs`
- `src/server/handlers/admin/connector_pack/import/planning.rs`
- `src/server/handlers/admin/connector_pack/import/skill_files.rs`
- `src/server/handlers/admin/connector_pack/import/validation.rs`
- `src/server/handlers/admin/connector_pack/reconcile.rs`
- `src/server/handlers/admin/connector_pack/reproject.rs`
- `src/server/handlers/admin/connector_pack/seed.rs`
- `src/server/handlers/admin/connector_pack/seed_tests.rs`
- `src/server/handlers/admin/connector_pack/self_served_catalog.rs`
- `src/server/handlers/admin/saga.rs`
- `src/server/handlers/admin/saga_tests.rs`
- `src/server/handlers/raft_admin.rs`
- `src/server/matview/lifecycle.rs`


## Codemod (run in this order, from the worktree root)
1. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-h-admin --moves > $SCRATCH/moves.sh`
2. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-h-admin --mods > $SCRATCH/mods.txt`
3. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-h-admin` rewrites `crate::<old path>` in the
   moved files and every caller, and points `placement.toml` at the new paths. Exit 1 prints the
   BLOCKED items and writes nothing.
4. `sh $SCRATCH/moves.sh` runs the `git mv` commands.
5. Apply `$SCRATCH/mods.txt`: add the `pub mod` lines to the new crate root and delete the listed
   `mod` declarations from the old parents. Move the crate's feature forwarding into its own
   `Cargo.toml` (the scaffold created the empty crate).
6. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-h-admin --check` must report nothing.

## Seams this lane uses (from SEAMS.md)
| category | item | refs | first location |
|---|---|---|---|
| extract-helper | `crate::server::dispatch::authoritative_now_ms` | 7 | src/server/connector_pack_projection.rs:191 |
| extract-helper | `crate::server::dispatch::blocking_task` | 6 | src/server/connector_pack_projection.rs:126 |
| extract-helper | `crate::server::dispatch::authoritative_now_secs` | 1 | src/server/handlers/admin/backup.rs:167 |
| extract-helper | `crate::server::dispatch::ensure_graph_resident` | 1 | src/server/handlers/admin/connector_pack/catalog_authority.rs:278 |
| extract-helper | `crate::server::dispatch::ensure_internal_global_graph` | 1 | src/server/graph_schema.rs:100 |
| extract-type | `crate::server::blob::BlobCursors` | 5 | src/server/handlers/admin/connector_pack.rs:295 |
| extract-type | `crate::server::blob::ChunkStore` | 4 | src/server/handlers/admin/connector_pack/import.rs:65 |
| extract-type | `crate::server::blob::engine_bodies::EngineBody` | 2 | src/server/handlers/admin/connector_pack/import.rs:14 |
| extract-type | `crate::server::blob::store::ChunkStore` | 2 | src/server/handlers/admin/connector_pack/reproject.rs:318 |
| extract-type | `crate::server::blob::store::HolderReconcile` | 2 | src/server/handlers/admin/connector_pack/reconcile.rs:160 |
| extract-type | `crate::server::blob::store::RedbChunkStore` | 2 | src/server/handlers/admin/connector_pack/reproject.rs:331 |
| extract-type | `crate::server::blob::engine_bodies::StoredEngineBody` | 1 | src/server/handlers/admin/connector_pack/import/planning.rs:46 |
| extract-type | `crate::server::blob::store::HolderId` | 1 | src/server/handlers/admin/connector_pack/reconcile.rs:160 |
| extract-type | `crate::server::blob::store::HolderNamespace` | 1 | src/server/handlers/admin/connector_pack/reconcile.rs:160 |
| needs-seam | `crate::server::dispatch::dispatch` | 8 | src/server/handlers/admin.rs:514 |
| needs-seam | `crate::server::mutation::MutationCtx` | 2 | src/server/graph_schema.rs:39 |
| needs-seam | `crate::server::mutation::MutationPlan` | 2 | src/server/graph_schema.rs:39 |
| needs-seam | `crate::MeasurementBatch` | 1 | src/server/connector_pack_projection/boot_regression.rs:422 |
| needs-seam | `crate::server::ShutdownCoordinator::new` | 1 | src/server/connector_pack_projection/boot_regression.rs:461 |
| needs-seam | `crate::server::boot_maintenance::spawn_post_bind_maintenance` | 1 | src/server/connector_pack_projection/boot_regression.rs:483 |
| needs-seam | `crate::server::dispatch::verified_served_registration` | 1 | src/server/handlers/admin/connector_pack/catalog_authority.rs:321 |
| needs-seam | `crate::server::serve_uds` | 1 | src/server/connector_pack_projection/boot_regression.rs:465 |
| seam:DecideApi | `crate::server::handlers::decide::topology_shape::refusal` | 1 | src/server/handlers/admin/agent.rs:597 |
| seam:GraphApi | `crate::server::handlers::graph_ops::commit_gateway` | 5 | src/server/graph_schema.rs:164 |
| seam:IngestApi | `crate::server::blob::engine_bodies::read_engine_body` | 4 | src/server/graph_schema/attach_pack.rs:54 |
| seam:IngestApi | `crate::server::blob::engine_bodies::engine_body_holder` | 1 | src/server/handlers/admin/connector_pack/reconcile.rs:159 |
| seam:IngestApi | `crate::server::blob::engine_bodies::engine_body_holder_namespace` | 1 | src/server/handlers/admin/connector_pack/reconcile.rs:159 |
| seam:IngestApi | `crate::server::handlers::blob::compile_blob_batch` | 1 | src/server/handlers/admin/connector_pack/reconcile.rs:126 |
| seam:IngestApi | `crate::server::handlers::blob::cursors_for_test` | 1 | src/server/connector_pack_projection/boot_regression.rs:409 |
| seam:TxnApi | `crate::server::txn::now_ms` | 1 | src/server/graph_schema/approved.rs:58 |

Use the server-api trait or extracted item named in the table. Do not invent another seam.

## Files it must not touch
Every file outside the list above, except caller rewrites from step 3. Do not change
`scripts/crate_split/*.toml` by hand; step 3 updates `placement.toml`. Do not copy helpers:
move them. Do not add re-exports. Widen `pub(crate)` to `pub` only where placement requires it.

## Gates (no local cargo)
- `python3 scripts/check_crate_graph.py` passes.
- `python3 scripts/crate_split/edges.py` shows no back-edge out of `medulla-h-admin` except the listed seams.
- `python3 scripts/check_spec_paths.py` passes; rewrite the `Spec:` test bindings and spec file paths
  that name moved files in the same commit, then regenerate spec status.
- `ruff check` and `ruff format --check` on touched Python.

## Land
```
eg-lane-run enqueue --lane land-h-admin --worktree <wt> --test '-p medulla-h-admin::all()'
```
The change lands on a passing build-train run of its changed crates. Link the train result in the
pull request. Commit trailer: `Spec: MEDULLA-CONTRACT-R056.8`.

## STOP conditions
- Any `BLOCKED` or `needs-seam` hit (from `rewrite_paths.py` or the seam table below) means STOP. Write `BLOCKED.md` in the lane directory (outside the repository) naming the item and the file:line, then exit. Do not improvise a seam.
- Any file outside the list above, other than caller rewrites made by `rewrite_paths.py`, means STOP.
- Cold pull-request CI timing out is not a blocker (MEDULLA-CONTRACT-R056.12).
- Never run cargo locally, never `git stash`, never `git add -A` or `git add .`, never `--no-verify`, never force-push.
