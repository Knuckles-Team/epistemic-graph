# Lane G1-server-api: move the files placed in `medulla-server-api`

Spec: `MEDULLA-CONTRACT-R056.5`. Start after: G0 landed; single lane.

## Base branch rule
Create the worktree from `main` after the previous group landed
(`git worktree add <wt> -b lane/crate-split-server-api origin/main`). Never branch from another lane.

## Files to move (70; exactly these, nothing else)
- `src/mutation_apply.rs`
- `src/server/access.rs`
- `src/server/access/read_classification.rs`
- `src/server/access/write_classification.rs`
- `src/server/anonymous.rs`
- `src/server/auth.rs`
- `src/server/auth/claims.rs`
- `src/server/authority_context.rs`
- `src/server/cdc.rs`
- `src/server/cdc/dispatch.rs`
- `src/server/cep.rs`
- `src/server/compute.rs`
- `src/server/denial_sample.rs`
- `src/server/http1.rs`
- `src/server/icv_guard.rs`
- `src/server/identity_view.rs`
- `src/server/impact_watch.rs`
- `src/server/impact_watch/grant.rs`
- `src/server/impact_watch/plan.rs`
- `src/server/matview/incremental.rs`
- `src/server/matview/lifecycle.rs`
- `src/server/matview/manager.rs`
- `src/server/matview/mod.rs`
- `src/server/mutation.rs`
- `src/server/mutation/coalescer.rs`
- `src/server/mutation/commit_encode.rs`
- `src/server/mutation/commit_entry.rs`
- `src/server/mutation/commit_paths.rs`
- `src/server/mutation/commit_replay.rs`
- `src/server/mutation/conditional.rs`
- `src/server/mutation/context.rs`
- `src/server/mutation/plan.rs`
- `src/server/mutation_batch.rs`
- `src/server/mutation_batch/canonical.rs`
- `src/server/mutation_batch/commit.rs`
- `src/server/mutation_batch/commit/envelope.rs`
- `src/server/mutation_batch/commit/internal.rs`
- `src/server/mutation_batch/commit/lifecycle.rs`
- `src/server/mutation_batch/commit/program.rs`
- `src/server/mutation_batch/commit/work_item.rs`
- `src/server/mutation_batch/compile.rs`
- `src/server/mutation_batch/compile/finish_helpers.rs`
- `src/server/mutation_batch/compile/sql_source_tests.rs`
- `src/server/mutation_batch/digest.rs`
- `src/server/mutation_batch/terminal_outcome.rs`
- `src/server/oidc.rs`
- `src/server/reasoning_cascade.rs`
- `src/server/request_replay.rs`
- `src/server/request_scope.rs`
- `src/server/routed_write_coalescer.rs`
- `src/server/service_grant.rs`
- `src/server/sparql_http.rs`
- `src/server/sparql_http/graph_store.rs`
- `src/server/sparql_http/negotiation.rs`
- `src/server/sparql_http/route.rs`
- `src/server/sparql_http/update_plan.rs`
- `src/server/sql_catalog_acl.rs`
- `src/server/sql_catalog_acl/ann_projection.rs`
- `src/server/sql_catalog_acl/identity_view.rs`
- `src/server/sql_catalog_acl/identity_view/tests.rs`
- `src/server/sql_catalog_acl/relations.rs`
- `src/server/sql_tables.rs`
- `src/server/sql_tables/ann_maintenance.rs`
- `src/server/state.rs`
- `src/server/txn.rs`
- `src/server/txn/modality.rs`
- `src/server/txn/recovery_plan.rs`
- `src/server/txn/recovery_plan_tests.rs`
- `src/server/txn/replay_intent.rs`
- `src/test_support.rs`


## Codemod (run in this order, from the worktree root)
1. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-server-api --moves > $SCRATCH/moves.sh`
2. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-server-api --mods > $SCRATCH/mods.txt`
3. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-server-api` rewrites `crate::<old path>` in the
   moved files and every caller, and points `placement.toml` at the new paths. Exit 1 prints the
   BLOCKED items and writes nothing.
4. `sh $SCRATCH/moves.sh` runs the `git mv` commands.
5. Apply `$SCRATCH/mods.txt`: add the `pub mod` lines to the new crate root and delete the listed
   `mod` declarations from the old parents. Move the crate's feature forwarding into its own
   `Cargo.toml` (the scaffold created the empty crate).
6. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-server-api --check` must report nothing.

## Seams this lane uses (from SEAMS.md)
| category | item | refs | first location |
|---|---|---|---|
| extract-helper | `crate::server::dispatch::authoritative_now_ms` | 12 | src/server/auth.rs:632 |
| extract-helper | `crate::server::dispatch::is_replicated_apply` | 2 | src/server/mutation/plan.rs:717 |
| extract-helper | `crate::server::graph_schema::compose::validate_and_compose` | 2 | src/server/icv_guard.rs:60 |
| extract-helper | `crate::server::dispatch::dispatch_verified_request` | 1 | src/server/impact_watch.rs:262 |
| extract-helper | `crate::server::dispatch::replicated_placement_authority` | 1 | src/server/mutation_batch/compile.rs:520 |
| extract-helper | `crate::server::outbound_http_config` | 1 | src/server/oidc.rs:371 |
| extract-helper | `crate::server::require_loopback_listener` | 1 | src/server/sparql_http.rs:110 |
| extract-type | `crate::server::persistence::PersistenceBackend` | 13 | src/server/mutation.rs:107 |
| extract-type | `crate::MeasurementBatch` | 1 | src/server/txn.rs:436 |
| extract-type | `crate::raft::RaftMutationContext` | 1 | src/server/authority_context.rs:108 |
| seam:dispatch | `crate::server::dispatch::dispatch` | 3 | src/server/sparql_http/graph_store.rs:121 |
| seam:dispatch | `crate::server::dispatch` | 1 | src/server/auth.rs:704 |
| seam:dispatch | `crate::server::dispatch::dispatch_authenticated_local_query` | 1 | src/server/sparql_http.rs:302 |
| seam:dispatch | `crate::server::handlers::graphlearn::replay` | 1 | src/mutation_apply.rs:163 |
| seam:dispatch | `crate::server::handlers::mining::replay` | 1 | src/mutation_apply.rs:157 |
| seam:state-handle | `crate::server::persistence::agent_library::AgentLibraryStore` | 5 | src/server/state.rs:185 |
| seam:state-handle | `crate::raft::RaftHandle` | 3 | src/server/state.rs:290 |
| seam:state-handle | `crate::raft::multi::MultiRaft` | 2 | src/server/state.rs:299 |
| seam:state-handle | `crate::raft::pregel::MatViewStore` | 1 | src/server/state.rs:337 |
| seam:state-handle | `crate::server::blob::BlobCursors` | 1 | src/server/state.rs:279 |
| seam:state-handle | `crate::server::foreign_catalog::ForeignSourceCatalog` | 1 | src/server/state.rs:352 |
| seam:state-handle | `crate::server::foreign_read::ForeignReadSessions` | 1 | src/server/state.rs:356 |
| seam:state-handle | `crate::server::kv::KvStore` | 1 | src/server/state.rs:365 |
| seam:state-handle | `crate::server::lake::LakeManager` | 1 | src/server/state.rs:375 |
| seam:state-handle | `crate::server::obs::ObsState` | 1 | src/server/state.rs:314 |
| seam:state-handle | `crate::server::persistence::PersistenceBackend` | 1 | src/server/state.rs:180 |
| seam:state-handle | `crate::server::persistence::agent_library::AgentLibraryStore::open` | 1 | src/server/state.rs:602 |
| seam:state-handle | `crate::server::persistence::cold_offload::ColdTenantTracker` | 1 | src/server/state.rs:198 |
| seam:state-handle | `crate::server::udf_catalog::UdfCatalog` | 1 | src/server/state.rs:331 |
| seam:state-handle | `crate::server::viz_engine::VizEngineState` | 1 | src/server/state.rs:207 |

Use the server-api trait or extracted item named in the table. Do not invent another seam.

This lane also owns every `extract-type`, `extract-helper` and `seam:state-handle` item in `SEAMS.md`, whatever file references it: move the item into `medulla-server-api` and rewrite its callers, so that later lanes find it below them.

## Files it must not touch
Every file outside the list above, except caller rewrites from step 3. Do not change
`scripts/crate_split/*.toml` by hand; step 3 updates `placement.toml`. Do not copy helpers:
move them. Do not add re-exports. Widen `pub(crate)` to `pub` only where placement requires it.

## Gates (no local cargo)
- `python3 scripts/check_crate_graph.py` passes.
- `python3 scripts/crate_split/edges.py` shows no back-edge out of `medulla-server-api` except the listed seams.
- `python3 scripts/check_spec_paths.py` passes; rewrite the `Spec:` test bindings and spec file paths
  that name moved files in the same commit, then regenerate spec status.
- `ruff check` and `ruff format --check` on touched Python.

## Land
```
eg-lane-run enqueue --lane land-server-api --worktree <wt> --test '-p medulla-server-api::all()'
```
The change lands on a passing build-train run of its changed crates. Link the train result in the
pull request. Commit trailer: `Spec: MEDULLA-CONTRACT-R056.5`.

## STOP conditions
- Any `BLOCKED` or `decide` hit (from `rewrite_paths.py` or the seam table below) means STOP. Write `BLOCKED.md` in the lane directory (outside the repository) naming the item and the file:line, then exit. Do not improvise a seam.
- Any file outside the list above, other than caller rewrites made by `rewrite_paths.py`, means STOP.
- Cold pull-request CI timing out is not a blocker (MEDULLA-CONTRACT-R056.12).
- Never run cargo locally, never `git stash`, never `git add -A` or `git add .`, never `--no-verify`, never force-push.
