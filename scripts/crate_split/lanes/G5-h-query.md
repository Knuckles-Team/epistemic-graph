# Lane G5-h-query: move the files placed in `medulla-h-query`

Spec: `MEDULLA-CONTRACT-R056.8`. Start after: G4 landed.

## Base branch rule
Create the worktree from `main` after the previous group landed
(`git worktree add <wt> -b lane/crate-split-h-query origin/main`). Never branch from another lane.

## Files to move (77; exactly these, nothing else)
- `src/server/federation/merge.rs`
- `src/server/federation/mod.rs`
- `src/server/federation/opensearch.rs`
- `src/server/foreign_catalog.rs`
- `src/server/foreign_read.rs`
- `src/server/foreign_share.rs`
- `src/server/handlers/federation.rs`
- `src/server/handlers/knowledge_stream/families.rs`
- `src/server/handlers/knowledge_stream/mod.rs`
- `src/server/handlers/knowledge_stream/stream.rs`
- `src/server/handlers/mining.rs`
- `src/server/handlers/mining/association.rs`
- `src/server/handlers/mining/classic.rs`
- `src/server/handlers/mining/entity.rs`
- `src/server/handlers/mining/impact.rs`
- `src/server/handlers/mining/impact/tests.rs`
- `src/server/handlers/mining/input.rs`
- `src/server/handlers/mining/insight.rs`
- `src/server/handlers/mining/process.rs`
- `src/server/handlers/mining/replay.rs`
- `src/server/handlers/mining/vector.rs`
- `src/server/handlers/mining/writeback.rs`
- `src/server/handlers/query.rs`
- `src/server/handlers/query/base.rs`
- `src/server/handlers/query/cypher.rs`
- `src/server/handlers/query/dispatch.rs`
- `src/server/handlers/query/dispatch/edge_index.rs`
- `src/server/handlers/query/explain_handlers.rs`
- `src/server/handlers/query/explain_wire_a.rs`
- `src/server/handlers/query/explain_wire_b.rs`
- `src/server/handlers/query/foreign_watermarks.rs`
- `src/server/handlers/query/freshness_feed.rs`
- `src/server/handlers/query/graphql.rs`
- `src/server/handlers/query/nl.rs`
- `src/server/handlers/query/overlay.rs`
- `src/server/handlers/query/planning.rs`
- `src/server/handlers/query/planning/federation_served_tests.rs`
- `src/server/handlers/query/planning/plan_deps_tests.rs`
- `src/server/handlers/query/sql_catalog.rs`
- `src/server/handlers/query/sql_dispatch.rs`
- `src/server/handlers/query/sql_read.rs`
- `src/server/handlers/query/sql_write.rs`
- `src/server/handlers/query/uql_statement.rs`
- `src/server/handlers/rdf.rs`
- `src/server/handlers/rdf/dispatch.rs`
- `src/server/handlers/rdf/inspect.rs`
- `src/server/handlers/rdf/inspect_tests.rs`
- `src/server/handlers/rdf/obda.rs`
- `src/server/handlers/rdf/reasoning.rs`
- `src/server/handlers/rdf/sparql.rs`
- `src/server/handlers/rdf/triples.rs`
- `src/server/handlers/rdf/typed_triples.rs`
- `src/server/handlers/rdf/validation.rs`
- `src/server/handlers/semantic_index.rs`
- `src/server/handlers/semantic_index/binding.rs`
- `src/server/handlers/semantic_index/contracts.rs`
- `src/server/handlers/semantic_index/dispatch.rs`
- `src/server/handlers/semantic_index/reads.rs`
- `src/server/handlers/semantic_index/source.rs`
- `src/server/handlers/semantic_index/worker.rs`
- `src/server/handlers/streaming.rs`
- `src/server/nl.rs`
- `src/server/reasoning_projection.rs`
- `src/server/reasoning_projection/tests/sweep_clock.rs`
- `src/server/request_cancel.rs`
- `src/server/secondary_indexes.rs`
- `src/server/semantic_index.rs`
- `src/server/semantic_index/activation.rs`
- `src/server/semantic_index/authority.rs`
- `src/server/semantic_index/dispatch.rs`
- `src/server/semantic_index/error_contract_tests.rs`
- `src/server/semantic_index/tenant_owner_upgrade_tests.rs`
- `src/server/semantic_index/upgrade.rs`
- `src/server/semantic_index/upgrade/activation_observation.rs`
- `src/server/semantic_index/upgrade/candidate.rs`
- `src/server/sparql_service/mod.rs`
- `src/server/sparql_service/qualification.rs`


## Codemod (run in this order, from the worktree root)
1. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-h-query --moves > $SCRATCH/moves.sh`
2. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-h-query --mods > $SCRATCH/mods.txt`
3. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-h-query` rewrites `crate::<old path>` in the
   moved files and every caller, and points `placement.toml` at the new paths. Exit 1 prints the
   BLOCKED items and writes nothing.
4. `sh $SCRATCH/moves.sh` runs the `git mv` commands.
5. Apply `$SCRATCH/mods.txt`: add the `pub mod` lines to the new crate root and delete the listed
   `mod` declarations from the old parents. Move the crate's feature forwarding into its own
   `Cargo.toml` (the scaffold created the empty crate).
6. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-h-query --check` must report nothing.

## Seams this lane uses (from SEAMS.md)
| category | item | refs | first location |
|---|---|---|---|
| extract-helper | `crate::server::dispatch::authoritative_now_ms` | 5 | src/server/handlers/query/foreign_watermarks.rs:58 |
| extract-helper | `crate::server::outbound_http_config` | 1 | src/server/federation/opensearch.rs:166 |
| extract-type | `crate::server::blob::cas_resolver::CasEvidenceResolver` | 2 | src/server/handlers/query/explain_wire_b.rs:281 |
| extract-type | `crate::server::sparql_http::ServiceClient` | 2 | src/server/sparql_service/mod.rs:133 |
| extract-type | `crate::server::blob::ChunkStore` | 1 | src/server/handlers/query/explain_wire_b.rs:319 |
| extract-type | `crate::server::graph_schema::terms::TermPageRequest` | 1 | src/server/handlers/rdf/dispatch.rs:337 |
| extract-type | `crate::server::handlers::decide::served_adapter::ServedAdapter` | 1 | src/server/handlers/query/planning.rs:702 |
| extract-type | `crate::server::handlers::web_ingest::embedder::EmbedderConfig` | 1 | src/server/handlers/query/planning.rs:21 |
| extract-type | `crate::server::handlers::web_ingest::embedder::REMOTE` | 1 | src/server/handlers/query/planning.rs:22 |
| extract-type | `crate::server::http1::HttpMessage` | 1 | src/server/federation/mod.rs:55 |
| extract-type | `crate::server::http1::RequestLimits` | 1 | src/server/federation/mod.rs:55 |
| needs-seam | `crate::server::sql_catalog_acl::AuthorizedReadStore::store` | 3 | src/server/handlers/query/sql_dispatch.rs:111 |
| needs-seam | `crate::server::sql_catalog_acl::RequestGraph` | 3 | src/server/handlers/query/dispatch.rs:531 |
| needs-seam | `crate::server::cdc::CdcHub` | 2 | src/server/handlers/streaming.rs:32 |
| needs-seam | `crate::server::sql_catalog_acl::AuthorizedReadStore` | 2 | src/server/handlers/query/sql_dispatch.rs:11 |
| needs-seam | `crate::server::sql_catalog_acl::open_authorized_table` | 2 | src/server/handlers/rdf/obda.rs:75 |
| needs-seam | `crate::server::dispatch::dispatch_authenticated_local_query` | 1 | src/server/federation/mod.rs:355 |
| needs-seam | `crate::server::dispatch::is_replicated_apply` | 1 | src/server/handlers/query/explain_handlers.rs:298 |
| needs-seam | `crate::server::sql_catalog_acl::ACCESS_DENIED` | 1 | src/server/handlers/query/sql_catalog.rs:55 |
| needs-seam | `crate::server::sql_catalog_acl::SemanticTextSnapshot` | 1 | src/server/semantic_index.rs:73 |
| needs-seam | `crate::server::sql_catalog_acl::SqlPrivilege` | 1 | src/server/semantic_index.rs:73 |
| needs-seam | `crate::server::sql_catalog_acl::SqlPrivilege::Select` | 1 | src/server/handlers/rdf/obda.rs:79 |
| needs-seam | `crate::server::sql_catalog_acl::alterable_ann_index_table` | 1 | src/server/handlers/query/sql_dispatch.rs:397 |
| needs-seam | `crate::server::sql_catalog_acl::authorized_read_store` | 1 | src/server/handlers/query/sql_dispatch.rs:39 |
| needs-seam | `crate::server::sql_catalog_acl::authorized_read_store_for_query` | 1 | src/server/handlers/query/sql_read.rs:436 |
| needs-seam | `crate::server::sql_catalog_acl::register_owner_after_create_in` | 1 | src/server/handlers/query/sql_catalog.rs:271 |
| needs-seam | `crate::server::sql_catalog_acl::stable_source_operation_id` | 1 | src/server/handlers/query/sql_catalog.rs:269 |
| needs-seam | `crate::server::sql_catalog_acl::with_source_authority_write` | 1 | src/server/handlers/query/sql_catalog.rs:232 |
| seam:AdminApi | `crate::server::graph_schema::handle_list` | 1 | src/server/handlers/rdf/dispatch.rs:325 |
| seam:AdminApi | `crate::server::graph_schema::terms::handle_classes` | 1 | src/server/handlers/rdf/dispatch.rs:333 |
| seam:DecideApi | `crate::server::handlers::decide::decision_source` | 1 | src/server/handlers/query/planning.rs:837 |
| seam:DecideApi | `crate::server::handlers::decide::read_only_relations` | 1 | src/server/handlers/query/sql_read.rs:430 |
| seam:DecideApi | `crate::server::handlers::decide::served_adapter::adapt_plan` | 1 | src/server/handlers/query/planning.rs:1031 |
| seam:DecideApi | `crate::server::handlers::decide::served_adapter::salt` | 1 | src/server/handlers/query/planning.rs:809 |
| seam:DecideApi | `crate::server::handlers::decide::served_adapter::served_plan_adapter` | 1 | src/server/handlers/query/planning.rs:739 |
| seam:TxnApi | `crate::server::handlers::txn` | 1 | src/server/handlers/query/graphql.rs:3 |
| seam:WireApi | `crate::server::wire::committed_sql_replay_receipt` | 2 | src/server/handlers/query/sql_catalog.rs:256 |
| seam:WireApi | `crate::server::http1` | 1 | src/server/federation/mod.rs:55 |
| seam:WireApi | `crate::server::wire::authorize_table_txn` | 1 | src/server/handlers/query/sql_catalog.rs:258 |
| seam:dispatch | `crate::server::handlers::TryHandleContext` | 2 | src/server/handlers/query/dispatch.rs:4 |

Use the server-api trait or extracted item named in the table. Do not invent another seam.

## Files it must not touch
Every file outside the list above, except caller rewrites from step 3. Do not change
`scripts/crate_split/*.toml` by hand; step 3 updates `placement.toml`. Do not copy helpers:
move them. Do not add re-exports. Widen `pub(crate)` to `pub` only where placement requires it.

## Gates (no local cargo)
- `python3 scripts/check_crate_graph.py` passes.
- `python3 scripts/crate_split/edges.py` shows no back-edge out of `medulla-h-query` except the listed seams.
- `python3 scripts/check_spec_paths.py` passes; rewrite the `Spec:` test bindings and spec file paths
  that name moved files in the same commit, then regenerate spec status.
- `ruff check` and `ruff format --check` on touched Python.

## Land
```
eg-lane-run enqueue --lane land-h-query --worktree <wt> --test '-p medulla-h-query::all()'
```
The change lands on a passing build-train run of its changed crates. Link the train result in the
pull request. Commit trailer: `Spec: MEDULLA-CONTRACT-R056.8`.

## STOP conditions
- Any `BLOCKED` or `needs-seam` hit (from `rewrite_paths.py` or the seam table below) means STOP. Write `BLOCKED.md` in the lane directory (outside the repository) naming the item and the file:line, then exit. Do not improvise a seam.
- Any file outside the list above, other than caller rewrites made by `rewrite_paths.py`, means STOP.
- Cold pull-request CI timing out is not a blocker (MEDULLA-CONTRACT-R056.12).
- Never run cargo locally, never `git stash`, never `git add -A` or `git add .`, never `--no-verify`, never force-push.
