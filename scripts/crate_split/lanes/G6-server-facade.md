# Lane G6-server-facade: move the files placed in `medulla-server`

Spec: `MEDULLA-CONTRACT-R056.10`. Start after: G5 landed.

## Base branch rule
Create the worktree from `main` after the previous group landed
(`git worktree add <wt> -b lane/crate-split-server origin/main`). Never branch from another lane.

## Files to move (174; exactly these, nothing else)
- `src/raft/command/tests.rs`
- `src/raft/tests.rs`
- `src/redb_store/work_item/run_owner/tests.rs`
- `src/server/cache_coherence.rs`
- `src/server/cdc_sink/mod.rs`
- `src/server/dispatch.rs`
- `src/server/dispatch/change_envelope.rs`
- `src/server/dispatch/change_envelope/multi_graph.rs`
- `src/server/dispatch/change_envelope_draft.rs`
- `src/server/dispatch/consensus.rs`
- `src/server/dispatch/consensus/fleet_catalog.rs`
- `src/server/dispatch/consensus/fleet_catalog/project.rs`
- `src/server/dispatch/consensus/fleet_catalog/project_tests.rs`
- `src/server/dispatch/consensus/fleet_catalog/read.rs`
- `src/server/dispatch/consensus/fleet_catalog/records.rs`
- `src/server/dispatch/consensus/fleet_catalog/write.rs`
- `src/server/dispatch/consensus/publication.rs`
- `src/server/dispatch/consensus/registry.rs`
- `src/server/dispatch/consensus/replicated.rs`
- `src/server/dispatch/consensus/routing.rs`
- `src/server/dispatch/consensus/sanitization.rs`
- `src/server/dispatch/consensus/transaction.rs`
- `src/server/dispatch/elevation.rs`
- `src/server/dispatch/elevation/tests.rs`
- `src/server/dispatch/governed.rs`
- `src/server/dispatch/governed/tests.rs`
- `src/server/dispatch/graph_pipeline.rs`
- `src/server/dispatch/graph_pipeline/dispatch_helpers.rs`
- `src/server/dispatch/graph_pipeline/gateway.rs`
- `src/server/dispatch/graph_pipeline/graph_access.rs`
- `src/server/dispatch/graph_pipeline/graph_dispatch.rs`
- `src/server/dispatch/graph_pipeline/modality.rs`
- `src/server/dispatch/graph_pipeline/native_routes.rs`
- `src/server/dispatch/graph_pipeline/pipeline.rs`
- `src/server/dispatch/graph_pipeline/repository_index.rs`
- `src/server/dispatch/graph_pipeline/repository_index/consumer.rs`
- `src/server/dispatch/graph_pipeline/repository_index/enrichment.rs`
- `src/server/dispatch/graph_pipeline/web_ingest.rs`
- `src/server/dispatch/graph_pipeline/work_governance.rs`
- `src/server/dispatch/identity_store.rs`
- `src/server/dispatch/identity_store/exposure.rs`
- `src/server/dispatch/identity_store/exposure/tests.rs`
- `src/server/dispatch/identity_store/pause.rs`
- `src/server/dispatch/identity_store/resolution_context.rs`
- `src/server/dispatch/identity_store/resolution_context/tests.rs`
- `src/server/dispatch/identity_store/secrets.rs`
- `src/server/dispatch/identity_store/secrets/tests.rs`
- `src/server/dispatch/identity_store/stamp.rs`
- `src/server/dispatch/identity_store/tests.rs`
- `src/server/dispatch/identity_store/tests/approval_keys.rs`
- `src/server/dispatch/identity_store/tests/refusal_contract.rs`
- `src/server/dispatch/identity_store/tests/revocation.rs`
- `src/server/dispatch/identity_store/tests/stale_verdict.rs`
- `src/server/dispatch/policy_evolution.rs`
- `src/server/dispatch/policy_evolution/blobs.rs`
- `src/server/dispatch/policy_evolution/blobs/tests.rs`
- `src/server/dispatch/policy_evolution/gate.rs`
- `src/server/dispatch/policy_evolution/gate/tests.rs`
- `src/server/dispatch/policy_evolution/store.rs`
- `src/server/dispatch/request_boundary.rs`
- `src/server/dispatch/request_boundary/authorization.rs`
- `src/server/dispatch/request_boundary/consensus.rs`
- `src/server/dispatch/request_boundary/method_errors.rs`
- `src/server/dispatch/request_boundary/preflight.rs`
- `src/server/dispatch/request_boundary/saga.rs`
- `src/server/dispatch/request_boundary/screen.rs`
- `src/server/dispatch/router.rs`
- `src/server/dispatch/router/channels.rs`
- `src/server/dispatch/router/control_plane.rs`
- `src/server/dispatch/router/data_plane.rs`
- `src/server/dispatch/router/data_plane_arms.rs`
- `src/server/dispatch/router/decision_plane.rs`
- `src/server/dispatch/router/graph_lifecycle.rs`
- `src/server/dispatch/router/identity_access.rs`
- `src/server/dispatch/router/lifecycle.rs`
- `src/server/dispatch/router/lifecycle/replicated_tenant_tests.rs`
- `src/server/dispatch/router/resource_cost.rs`
- `src/server/dispatch/router/service_control.rs`
- `src/server/dispatch/router/source_ingest.rs`
- `src/server/dispatch/router/telemetry.rs`
- `src/server/dispatch/sparql_update.rs`
- `src/server/dispatch/telemetry.rs`
- `src/server/dispatch/telemetry/classes.rs`
- `src/server/dispatch/telemetry/collect.rs`
- `src/server/dispatch/telemetry/declarations.rs`
- `src/server/dispatch/telemetry/materialize.rs`
- `src/server/dispatch/telemetry/tests.rs`
- `src/server/dispatch/test_support.rs`
- `src/server/graph_schema/compiled_tests.rs`
- `src/server/graphql_sub.rs`
- `src/server/handlers/admin/connector_pack/tests.rs`
- `src/server/handlers/admin/connector_pack/tests/admin.rs`
- `src/server/handlers/admin/connector_pack/tests/content_model.rs`
- `src/server/handlers/admin/connector_pack/tests/model_input_capabilities.rs`
- `src/server/handlers/admin/connector_pack/tests/ontology.rs`
- `src/server/handlers/admin/connector_pack/tests/outbox.rs`
- `src/server/handlers/admin/connector_pack/tests/scale.rs`
- `src/server/handlers/admin/connector_pack/tests/self_served_catalog.rs`
- `src/server/handlers/admin/connector_pack/tests/source_approval.rs`
- `src/server/handlers/admin/connector_pack/tests/storage_acceptance.rs`
- `src/server/handlers/admin/connector_pack/tests/storage_acceptance/receipt.rs`
- `src/server/handlers/admin/connector_pack/tests/withdrawal.rs`
- `src/server/handlers/decide/guardrail_tests.rs`
- `src/server/handlers/decide/stat_tests.rs`
- `src/server/handlers/decide/stat_tests/attribution_tests.rs`
- `src/server/handlers/decide/stat_tests/classes_tests.rs`
- `src/server/handlers/decide/stat_tests/connector_toolchoice_tests.rs`
- `src/server/handlers/decide/stat_tests/connector_triage_tests.rs`
- `src/server/handlers/decide/stat_tests/connector_writeback_tests.rs`
- `src/server/handlers/decide/stat_tests/consumer_tests.rs`
- `src/server/handlers/decide/stat_tests/evaluator_tests.rs`
- `src/server/handlers/decide/stat_tests/harness_config_tests.rs`
- `src/server/handlers/decide/stat_tests/learning_tests.rs`
- `src/server/handlers/decide/stat_tests/log_tests.rs`
- `src/server/handlers/decide/stat_tests/nl_tests.rs`
- `src/server/handlers/decide/stat_tests/pretool_risk_tests.rs`
- `src/server/handlers/decide/stat_tests/replay_tests.rs`
- `src/server/handlers/decide/stat_tests/reputation_tests.rs`
- `src/server/handlers/decide/stat_tests/retention_tests.rs`
- `src/server/handlers/decide/stat_tests/retrieval_tests.rs`
- `src/server/handlers/decide/stat_tests/scorer_tests.rs`
- `src/server/handlers/decide/stat_tests/task_plan_tests.rs`
- `src/server/handlers/decide/stat_tests/tool_subset_tests.rs`
- `src/server/handlers/jobs/graph_tests.rs`
- `src/server/handlers/knowledge_stream/tests.rs`
- `src/server/handlers/mod.rs`
- `src/server/handlers/repository_index_tests.rs`
- `src/server/handlers/timeseries/nested_payload_tests.rs`
- `src/server/handlers/txn/consensus/authority_tests.rs`
- `src/server/handlers/web_ingest_tests.rs`
- `src/server/impact_watch/tests.rs`
- `src/server/issuer_trust.rs`
- `src/server/lake/catalog_ops.rs`
- `src/server/lake/lineage.rs`
- `src/server/lake/lineage_transport.rs`
- `src/server/lake/mod.rs`
- `src/server/lake/rest/catalog.rs`
- `src/server/lake/rest/mod.rs`
- `src/server/mod.rs`
- `src/server/mutation_batch/lifecycle_tests.rs`
- `src/server/mutation_batch/tests.rs`
- `src/server/obda/mod.rs`
- `src/server/obs/http/mod.rs`
- `src/server/obs/http/search.rs`
- `src/server/obs/manifests.rs`
- `src/server/obs/mod.rs`
- `src/server/obs/otel_export.rs`
- `src/server/obs/parse.rs`
- `src/server/obs/remote_write.rs`
- `src/server/obs/retention.rs`
- `src/server/obs/search.rs`
- `src/server/obs/segment.rs`
- `src/server/obs/snapshot.rs`
- `src/server/obs/state.rs`
- `src/server/obs/writer.rs`
- `src/server/obs/writer/tests.rs`
- `src/server/policy_export/mod.rs`
- `src/server/promql/mod.rs`
- `src/server/qos.rs`
- `src/server/registry_reaper.rs`
- `src/server/replica.rs`
- `src/server/sealed_retention.rs`
- `src/server/sealed_retention/tests.rs`
- `src/server/semantic_activation.rs`
- `src/server/semantic_index/dispatch_pipeline_tests.rs`
- `src/server/semantic_index/sql_source_read_tests.rs`
- `src/server/tests/foreign_tenancy.rs`
- `src/server/tests/nl_explain.rs`
- `src/server/tests/shex_served.rs`
- `src/server/tests/udf_tenancy.rs`
- `src/server/traces/mod.rs`
- `src/server/transport/tests.rs`
- `src/server/udf_catalog.rs`
- `src/server_startup.rs`

## Test files that land under `crates/medulla-server/tests/`
- `src/raft/command/tests.rs`
- `src/raft/tests.rs`
- `src/redb_store/work_item/run_owner/tests.rs`
- `src/server/graph_schema/compiled_tests.rs`
- `src/server/handlers/admin/connector_pack/tests.rs`
- `src/server/handlers/admin/connector_pack/tests/admin.rs`
- `src/server/handlers/admin/connector_pack/tests/content_model.rs`
- `src/server/handlers/admin/connector_pack/tests/model_input_capabilities.rs`
- `src/server/handlers/admin/connector_pack/tests/ontology.rs`
- `src/server/handlers/admin/connector_pack/tests/outbox.rs`
- `src/server/handlers/admin/connector_pack/tests/scale.rs`
- `src/server/handlers/admin/connector_pack/tests/self_served_catalog.rs`
- `src/server/handlers/admin/connector_pack/tests/source_approval.rs`
- `src/server/handlers/admin/connector_pack/tests/storage_acceptance.rs`
- `src/server/handlers/admin/connector_pack/tests/storage_acceptance/receipt.rs`
- `src/server/handlers/admin/connector_pack/tests/withdrawal.rs`
- `src/server/handlers/decide/guardrail_tests.rs`
- `src/server/handlers/decide/stat_tests.rs`
- `src/server/handlers/decide/stat_tests/attribution_tests.rs`
- `src/server/handlers/decide/stat_tests/classes_tests.rs`
- `src/server/handlers/decide/stat_tests/connector_toolchoice_tests.rs`
- `src/server/handlers/decide/stat_tests/connector_triage_tests.rs`
- `src/server/handlers/decide/stat_tests/connector_writeback_tests.rs`
- `src/server/handlers/decide/stat_tests/consumer_tests.rs`
- `src/server/handlers/decide/stat_tests/evaluator_tests.rs`
- `src/server/handlers/decide/stat_tests/harness_config_tests.rs`
- `src/server/handlers/decide/stat_tests/learning_tests.rs`
- `src/server/handlers/decide/stat_tests/log_tests.rs`
- `src/server/handlers/decide/stat_tests/nl_tests.rs`
- `src/server/handlers/decide/stat_tests/pretool_risk_tests.rs`
- `src/server/handlers/decide/stat_tests/replay_tests.rs`
- `src/server/handlers/decide/stat_tests/reputation_tests.rs`
- `src/server/handlers/decide/stat_tests/retention_tests.rs`
- `src/server/handlers/decide/stat_tests/retrieval_tests.rs`
- `src/server/handlers/decide/stat_tests/scorer_tests.rs`
- `src/server/handlers/decide/stat_tests/task_plan_tests.rs`
- `src/server/handlers/decide/stat_tests/tool_subset_tests.rs`
- `src/server/handlers/jobs/graph_tests.rs`
- `src/server/handlers/knowledge_stream/tests.rs`
- `src/server/handlers/repository_index_tests.rs`
- `src/server/handlers/timeseries/nested_payload_tests.rs`
- `src/server/handlers/txn/consensus/authority_tests.rs`
- `src/server/handlers/web_ingest_tests.rs`
- `src/server/impact_watch/tests.rs`
- `src/server/mutation_batch/lifecycle_tests.rs`
- `src/server/mutation_batch/tests.rs`
- `src/server/semantic_index/dispatch_pipeline_tests.rs`
- `src/server/semantic_index/sql_source_read_tests.rs`
- `src/server/transport/tests.rs`

## Codemod (run in this order, from the worktree root)
1. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-server --moves > $SCRATCH/moves.sh`
2. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-server --mods > $SCRATCH/mods.txt`
3. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-server` rewrites `crate::<old path>` in the
   moved files and every caller, and points `placement.toml` at the new paths. Exit 1 prints the
   BLOCKED items and writes nothing.
4. `sh $SCRATCH/moves.sh` runs the `git mv` commands.
5. Apply `$SCRATCH/mods.txt`: add the `pub mod` lines to the new crate root and delete the listed
   `mod` declarations from the old parents. Move the crate's feature forwarding into its own
   `Cargo.toml` (the scaffold created the empty crate).
6. `python3 scripts/crate_split/rewrite_paths.py --crate medulla-server --check` must report nothing.

## Seams this lane uses (from SEAMS.md)
| category | item | refs | first location |
|---|---|---|---|
| pending-delete | `crate::server::contract_wave::PENDING_METHODS` | 1 | src/server/dispatch/router/service_control.rs:150 |

Use the server-api trait or extracted item named in the table. Do not invent another seam.

## Files it must not touch
Every file outside the list above, except caller rewrites from step 3. Do not change
`scripts/crate_split/*.toml` by hand; step 3 updates `placement.toml`. Do not copy helpers:
move them. Do not add re-exports. Widen `pub(crate)` to `pub` only where placement requires it.

## Gates (no local cargo)
- `python3 scripts/check_crate_graph.py` passes.
- `python3 scripts/crate_split/edges.py` shows no back-edge out of `medulla-server` except the listed seams.
- `python3 scripts/check_spec_paths.py` passes; rewrite the `Spec:` test bindings and spec file paths
  that name moved files in the same commit, then regenerate spec status.
- `ruff check` and `ruff format --check` on touched Python.

## Land
```
eg-lane-run enqueue --lane land-server --worktree <wt> --test '-p medulla-server::all()'
```
The change lands on a passing build-train run of its changed crates. Link the train result in the
pull request. Commit trailer: `Spec: MEDULLA-CONTRACT-R056.10`.

## STOP conditions
- Any `BLOCKED` or `decide` hit (from `rewrite_paths.py` or the seam table below) means STOP. Write `BLOCKED.md` in the lane directory (outside the repository) naming the item and the file:line, then exit. Do not improvise a seam.
- Any file outside the list above, other than caller rewrites made by `rewrite_paths.py`, means STOP.
- Cold pull-request CI timing out is not a blocker (MEDULLA-CONTRACT-R056.12).
- Never run cargo locally, never `git stash`, never `git add -A` or `git add .`, never `--no-verify`, never force-push.
