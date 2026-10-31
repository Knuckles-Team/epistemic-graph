# Lane G3-wire: move the files placed in `medulla-wire`

Spec: `MEDULLA-CONTRACT-R056.9`. Start after: G2 landed.

## Base branch rule
Create the worktree from `main` after the previous group landed
(`git worktree add <wt> -b lane/crate-split-wire origin/main`). Never branch from another lane.

## Files to move (31; exactly these, nothing else)
- `src/server/amqp_wire/codec.rs`
- `src/server/amqp_wire/mod.rs`
- `src/server/bolt_wire/auth.rs`
- `src/server/bolt_wire/mod.rs`
- `src/server/bolt_wire/packstream.rs`
- `src/server/bolt_wire/session.rs`
- `src/server/broker_wire.rs`
- `src/server/dds.rs`
- `src/server/kvcache_http/fork.rs`
- `src/server/kvcache_http/mod.rs`
- `src/server/kvcache_http/shared_store.rs`
- `src/server/mqtt_wire/codec.rs`
- `src/server/mqtt_wire/mod.rs`
- `src/server/mssql_wire/mod.rs`
- `src/server/mssql_wire/protocol.rs`
- `src/server/mysql_wire/auth.rs`
- `src/server/mysql_wire/mod.rs`
- `src/server/mysql_wire/packets.rs`
- `src/server/pgwire/auth.rs`
- `src/server/pgwire/mod.rs`
- `src/server/redis_wire/mod.rs`
- `src/server/ros2_bridge.rs`
- `src/server/s3/calendar.rs`
- `src/server/s3/mod.rs`
- `src/server/s3/route.rs`
- `src/server/sql_wire_auth.rs`
- `src/server/sqlite_wire/dialect.rs`
- `src/server/sqlite_wire/mod.rs`
- `src/server/stomp_wire/mod.rs`
- `src/server/wire/mod.rs`
- `src/server/wire/wired_catalog_tests/edge_index.rs`


## Codemod (run in this order, from the worktree root)
1. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-wire --moves > $SCRATCH/moves.sh`
2. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-wire --mods > $SCRATCH/mods.txt`
3. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-wire` rewrites `crate::<old path>` in the
   moved files and every caller, and points `placement.toml` at the new paths. Exit 1 prints the
   BLOCKED items and writes nothing.
4. `sh $SCRATCH/moves.sh` runs the `git mv` commands.
5. Apply `$SCRATCH/mods.txt`: add the `pub mod` lines to the new crate root and delete the listed
   `mod` declarations from the old parents. Move the crate's feature forwarding into its own
   `Cargo.toml` (the scaffold created the empty crate).
6. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-wire --check` must report nothing.

## Seams this lane uses (from SEAMS.md)
| category | item | refs | first location |
|---|---|---|---|
| extract-helper | `crate::server::validate_direct_wire_security` | 7 | src/server/amqp_wire/mod.rs:88 |
| extract-helper | `crate::server::pseudonymous_broker_actor` | 5 | src/server/amqp_wire/mod.rs:799 |
| extract-helper | `crate::server::require_loopback_listener` | 2 | src/server/kvcache_http/mod.rs:810 |
| extract-helper | `crate::server::dispatch::dispatch_authenticated_broker_actor` | 1 | src/server/broker_wire.rs:11 |
| extract-helper | `crate::server::unique_temp_dir` | 1 | src/server/broker_wire.rs:73 |
| extract-type | `crate::server::txn_intent::ReplayStep` | 8 | src/server/wire/mod.rs:1245 |
| extract-type | `crate::server::txn_intent::CommitIntent` | 4 | src/server/wire/mod.rs:2601 |
| extract-type | `crate::server::handlers::query::TsdbLegBind` | 1 | src/server/wire/mod.rs:4659 |
| extract-type | `crate::server::handlers::query::current_auth_test_support::EDGE_STATUS_SQL` | 1 | src/server/wire/wired_catalog_tests/edge_index.rs:9 |
| seam:DecideApi | `crate::server::handlers::decide::read_only_relations` | 1 | src/server/wire/mod.rs:1840 |
| seam:QueryApi | `crate::server::foreign_catalog::bound_registry` | 1 | src/server/wire/mod.rs:4657 |
| seam:QueryApi | `crate::server::handlers::query::bind_sql_text_embedder` | 1 | src/server/pgwire/mod.rs:802 |
| seam:QueryApi | `crate::server::handlers::query::current_auth_test_support::edge_status_row` | 1 | src/server/wire/wired_catalog_tests/edge_index.rs:9 |
| seam:QueryApi | `crate::server::handlers::query::current_auth_test_support::register_detached_edge_index` | 1 | src/server/wire/wired_catalog_tests/edge_index.rs:9 |
| seam:QueryApi | `crate::server::handlers::query::current_auth_test_support::sql_rows` | 1 | src/server/wire/wired_catalog_tests/edge_index.rs:9 |
| seam:QueryApi | `crate::server::handlers::query::execute_rows` | 1 | src/server/wire/mod.rs:4665 |
| seam:QueryApi | `crate::server::handlers::query::overlay_write_set` | 1 | src/server/wire/mod.rs:4622 |
| seam:QueryApi | `crate::server::handlers::query::plan_needs_tsdb` | 1 | src/server/wire/mod.rs:4591 |
| seam:QueryApi | `crate::server::handlers::query::run_unified_with_staged_finish` | 1 | src/server/wire/mod.rs:4651 |
| seam:TxnApi | `crate::server::txn_intent::delete_intent` | 4 | src/server/wire/mod.rs:2676 |
| seam:TxnApi | `crate::server::handlers::txn::commit_cross_modal_txn_with_nonce` | 2 | src/server/wire/mod.rs:4133 |
| seam:TxnApi | `crate::server::txn_intent::write_intent` | 2 | src/server/wire/mod.rs:2615 |
| seam:TxnApi | `crate::server::handlers::txn::construct_view_to_methods` | 1 | src/server/wire/mod.rs:4522 |
| seam:TxnApi | `crate::server::handlers::txn::sparql_update_to_methods` | 1 | src/server/wire/mod.rs:4501 |
| seam:TxnApi | `crate::server::txn_intent::list_intents` | 1 | src/server/wire/mod.rs:2868 |
| seam:dispatch | `crate::server::dispatch` | 1 | src/server/wire/wired_catalog_tests/edge_index.rs:93 |
| seam:dispatch | `crate::server::dispatch::dispatch` | 1 | src/server/ros2_bridge.rs:229 |
| seam:state-handle | `crate::server::blob::store::ChunkStore` | 1 | src/server/s3/mod.rs:61 |
| seam:state-handle | `crate::server::blob::store::RedbChunkStore` | 1 | src/server/s3/mod.rs:61 |

Use the server-api trait or extracted item named in the table. Do not invent another seam.

## Files it must not touch
Every file outside the list above, except caller rewrites from step 3. Do not change
`scripts/crate_split/*.toml` by hand; step 3 updates `placement.toml`. Do not copy helpers:
move them. Do not add re-exports. Widen `pub(crate)` to `pub` only where placement requires it.

## Gates (no local cargo)
- `python3 scripts/check_crate_graph.py` passes.
- `python3 scripts/crate_split/edges.py` shows no back-edge out of `medulla-wire` except the listed seams.
- `python3 scripts/check_spec_paths.py` passes; rewrite the `Spec:` test bindings and spec file paths
  that name moved files in the same commit, then regenerate spec status.
- `ruff check` and `ruff format --check` on touched Python.

## Land
```
eg-lane-run enqueue --lane land-wire --worktree <wt> --test '-p medulla-wire::all()'
```
The change lands on a passing build-train run of its changed crates. Link the train result in the
pull request. Commit trailer: `Spec: MEDULLA-CONTRACT-R056.9`.

## STOP conditions
- Any `BLOCKED` or `decide` hit (from `rewrite_paths.py` or the seam table below) means STOP. Write `BLOCKED.md` in the lane directory (outside the repository) naming the item and the file:line, then exit. Do not improvise a seam.
- Any file outside the list above, other than caller rewrites made by `rewrite_paths.py`, means STOP.
- Cold pull-request CI timing out is not a blocker (MEDULLA-CONTRACT-R056.12).
- Never run cargo locally, never `git stash`, never `git add -A` or `git add .`, never `--no-verify`, never force-push.
