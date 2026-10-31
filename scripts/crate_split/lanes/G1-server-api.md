# Lane G1-server-api: move the files placed in `medulla-server-api`

Spec: `MEDULLA-CONTRACT-R056.5`. Start after: G0 landed; single lane.

## Base branch rule
Create the worktree from `main` after the previous group landed
(`git worktree add <wt> -b lane/crate-split-server-api origin/main`). Never branch from another lane.

## Files to move (32; exactly these, nothing else)
- `src/raft/mod.rs`
- `src/server/access.rs`
- `src/server/access/read_classification.rs`
- `src/server/access/write_classification.rs`
- `src/server/anonymous.rs`
- `src/server/auth.rs`
- `src/server/auth/claims.rs`
- `src/server/authority_context.rs`
- `src/server/compute.rs`
- `src/server/denial_sample.rs`
- `src/server/identity_view.rs`
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
- `src/server/mutation_batch/tests.rs`
- `src/server/outbox_operator.rs`
- `src/server/persistence/contract.rs`
- `src/server/request_replay.rs`
- `src/server/request_scope.rs`
- `src/server/service_grant.rs`
- `src/server/state.rs`
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
| extract-helper | `crate::server::dispatch::authoritative_now_ms` | 6 | src/server/auth.rs:632 |
| needs-seam | `crate::server::oidc::JwtValidator` | 5 | src/server/auth.rs:175 |
| needs-seam | `crate::server::oidc::VerifiedTokenClaims` | 2 | src/server/auth.rs:143 |
| needs-seam | `crate::MeasurementBatch` | 1 | src/server/persistence/contract.rs:753 |
| needs-seam | `crate::server::dispatch::replicated_placement_authority` | 1 | src/server/mutation_batch/compile.rs:520 |
| needs-seam | `crate::server::mutation::LifecycleAttempt` | 1 | src/server/mutation_batch/commit/lifecycle.rs:5 |
| needs-seam | `crate::server::mutation::publish_committed_row_delta` | 1 | src/server/mutation_batch/commit/internal.rs:441 |
| needs-seam | `crate::server::oidc::JwtValidator::from_env_primary` | 1 | src/server/auth.rs:862 |
| needs-seam | `crate::server::persistence::CrossModalCommitArgs` | 1 | src/server/persistence/contract.rs:5 |
| needs-seam | `crate::server::persistence::redb_backend` | 1 | src/server/persistence/contract.rs:4 |
| seam:dispatch | `crate::server::dispatch` | 1 | src/server/auth.rs:704 |
| seam:state-handle | `crate::server::persistence::agent_library::AgentLibraryStore` | 5 | src/server/state.rs:185 |
| seam:state-handle | `crate::raft::multi::MultiRaft` | 2 | src/server/state.rs:299 |
| seam:state-handle | `crate::raft::pregel::MatViewStore` | 1 | src/server/state.rs:337 |
| seam:state-handle | `crate::server::blob::BlobCursors` | 1 | src/server/state.rs:279 |
| seam:state-handle | `crate::server::cdc::CdcHub` | 1 | src/server/state.rs:322 |
| seam:state-handle | `crate::server::foreign_catalog::ForeignSourceCatalog` | 1 | src/server/state.rs:352 |
| seam:state-handle | `crate::server::foreign_read::ForeignReadSessions` | 1 | src/server/state.rs:356 |
| seam:state-handle | `crate::server::kv::KvStore` | 1 | src/server/state.rs:365 |
| seam:state-handle | `crate::server::lake::LakeManager` | 1 | src/server/state.rs:375 |
| seam:state-handle | `crate::server::obs::ObsState` | 1 | src/server/state.rs:314 |
| seam:state-handle | `crate::server::persistence::agent_library::AgentLibraryStore::open` | 1 | src/server/state.rs:602 |
| seam:state-handle | `crate::server::persistence::cold_offload::ColdTenantTracker` | 1 | src/server/state.rs:198 |
| seam:state-handle | `crate::server::routed_write_coalescer::RoutedWriteCoalescerRegistry` | 1 | src/server/state.rs:250 |
| seam:state-handle | `crate::server::txn::GraphTxnState` | 1 | src/server/state.rs:10 |
| seam:state-handle | `crate::server::txn::TxnIdGen` | 1 | src/server/state.rs:10 |
| seam:state-handle | `crate::server::udf_catalog::UdfCatalog` | 1 | src/server/state.rs:331 |
| seam:state-handle | `crate::server::viz_engine::VizEngineState` | 1 | src/server/state.rs:207 |

Use the server-api trait or extracted item named in the table. Do not invent another seam.

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
- Any `BLOCKED` or `needs-seam` hit (from `rewrite_paths.py` or the seam table below) means STOP. Write `BLOCKED.md` in the lane directory (outside the repository) naming the item and the file:line, then exit. Do not improvise a seam.
- Any file outside the list above, other than caller rewrites made by `rewrite_paths.py`, means STOP.
- Cold pull-request CI timing out is not a blocker (MEDULLA-CONTRACT-R056.12).
- Never run cargo locally, never `git stash`, never `git add -A` or `git add .`, never `--no-verify`, never force-push.
