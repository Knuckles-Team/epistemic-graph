# Owner-store formats

Generated from `crates/eg-storage/src/owner/lineage.rs`; do not edit by hand.
Regenerate with `cargo run -p eg-storage --example gen_owner_store_formats`.

Every durable owner file records the digest of its exact table layout. A file whose layout is a declared predecessor below is refused on ordinary open with the named error. The SQL, immediate pre-MCP Agent Library, pre-enrichment GraphShard and immediate pre-audit-request-index GraphShard predecessors have explicit offline, data-preserving upgrades; others move aside and re-created. Any other digest is refused as `OWNER_STORE_FORMAT_UNKNOWN`.

| Store | Current layout digest | Refused predecessors |
|---|---|---|
| `ledger_only` | `9faad03bf787a2a893618b7a8b386de30bdd5b4e09f3930448040c3e47c59e3e` | none |
| `rbac` | `3c89b3207faa962a94eb1ce724f944dde5a4acf94841a23d9c416855866d4c8e` | none |
| `jobs` | `64eb7ffd327980a8269bc8cf61147bc269b4ee8eab1ae0f5de3a244bbec9dad4` | none |
| `statechart` | `d8ab661d50af9249a342a101537f0ef92c13df6808b5fd149c037d8587cff8a7` | none |
| `time_series` | `5f2b379c6ea02f36f38bcfd2fd8f6277e5174467f3c81b309fffdf87ec1a1581` | none |
| `kv` | `48b6464ad5960bd14fc0976119b689c0016be63e8cc8be72df4e7030ed593f9e` | none |
| `blob` | `982e463fd13db3ec48fbdda77249ecaefed3b9174276c61ac7cd2eb206c47d6b` | `BLOB_FORMAT_UPGRADE_REQUIRED` |
| `semantic_index` | `3008d8726d15a864daf6c723004152fc61bb40bc8de0fea486dfdaace0c985d8` | none |
| `sql` | `7435c69e2de7052a351a89f17ccb4a988f92189f09d523ee7f8e088607924398` | `SQL_FORMAT_UPGRADE_REQUIRED` |
| `path_index` | `c79d89a41ad97d55cf35238ca8706db8c958923624de068a430c3bca72656df9` | none |
| `request_replay` | `ccb7051fef1a624274939944f7af9ee0f3ee03eee062794eeaf105d1c6117809` | none |
| `viz_provenance` | `57e566cd7fae6e67c83778cb50423130b8c250c2ce58dd3e93f5b203151661d3` | none |
| `cold_tier` | `9170bbb3960d0eddb71c4b080878abcb4440698472124911e78125aa6f3931ad` | none |
| `tenant_catalog` | `01b945d16fa187e69ca3dd5777a27066310b64a8b351380af1ba810f5dcd57aa` | none |
| `node_info` | `02e5ca262822f4de0a1507780b2e908e605655d3a758e78c2a75f205ffa80347` | none |
| `cluster_hierarchy` | `0561a2a2ae13f067bf01a4c94d6cbaeb280aaefa56c68036a1f01da92cba8431` | none |
| `graph_shard` | `952ea729e026deee49d9922dd2577cd1f789860be06b19e981cbb2949cf2a217` | `GRAPH_SHARD_FORMAT_UPGRADE_REQUIRED` |
| `agent_library` | `e112d8118e4e62b002799b84bba8c69162e477071861a81d3893277736727ea0` | `AGENT_LIBRARY_FORMAT_UPGRADE_REQUIRED` |

## `BLOB_FORMAT_UPGRADE_REQUIRED`: blob store from before holder-scoped references

* Store file: `blob.redb`
* Refused generation: blob store from before holder-scoped references
* Data lost: its chunks, manifests, reference counts and uploads are not migrated, so media blobs referenced by graphs must be uploaded again
* Owner tables of the refused generation: `cas_chunks`, `cas_blobs`, `cas_refcount`, `cas_uploads`
* Removal step: stop the engine, move `blob.redb` aside (keep it until the restarted engine is confirmed healthy), and restart; a fresh store is created. Backup bundles taken before this release cannot restore this file.

## `SQL_FORMAT_UPGRADE_REQUIRED`: SQL catalog store before durable source checkpoints

* Store file: `sql.redb`
* Refused generation: SQL catalog store before durable source checkpoints
* Data lost: its SQL catalog and rows require an explicit offline upgrade before normal open, or re-ingestion after moving the file aside
* Owner tables of the refused generation: `__sql_catalog__`, `__sql_functions__`, `__sql_ann_indexes__`, `__sql_secondary_indexes__`, `__sql_secondary_index_entries__`, `__sql_hypertables__`, `__sql_source_authority__`, `__sql_views__`, `__sql_extensions__`, `__sql_rows__`, `__sql_seq__`, `__sql_schema_catalog_versions__`, `__sql_schema_versions__`, `__sql_schema_migrations__`, `__sql_schema_migration_order__`, `__sql_schema_catalog_order__`, `__sql_property_graphs__`, `__sql_property_graph_seq__`
* Removal step: none. This generation is upgraded in place with `epistemic-graph-server store-upgrade apply <data-dir> --confirm` while the engine is stopped; see the procedure below. Moving the file aside instead loses its rows.

## `SQL_FORMAT_UPGRADE_REQUIRED`: SQL catalog store before durable ANN and edge index generations

* Store file: `sql.redb`
* Refused generation: SQL catalog store before durable ANN and edge index generations
* Data lost: its SQL catalog and rows require an explicit offline upgrade before normal open, or re-ingestion after moving the file aside
* Owner tables of the refused generation: `__sql_catalog__`, `__sql_functions__`, `__sql_ann_indexes__`, `__sql_secondary_indexes__`, `__sql_secondary_index_entries__`, `__sql_hypertables__`, `__sql_source_authority__`, `__sql_views__`, `__sql_extensions__`, `__sql_rows__`, `__sql_seq__`, `__sql_schema_catalog_versions__`, `__sql_schema_versions__`, `__sql_schema_migrations__`, `__sql_schema_migration_order__`, `__sql_schema_catalog_order__`, `__sql_property_graphs__`, `__sql_property_graph_seq__`, `__sql_source_checkpoints__`
* Removal step: none. This generation is upgraded in place with `epistemic-graph-server store-upgrade apply <data-dir> --confirm` while the engine is stopped; see the procedure below. Moving the file aside instead loses its rows.

## `GRAPH_SHARD_FORMAT_UPGRADE_REQUIRED`: graph shard store before the background node-payload scrub

* Store file: `graph-*.redb`
* Refused generation: graph shard store before the background node-payload scrub
* Data lost: its graphs are not migrated; re-ingest the sources
* Owner tables of the refused generation: `nodes`, `edges`, `ledger`, `semantic_store`, `audit_chain`, `provenance_anchor_members`, `graph_meta`, `work_item_command_sequence`, `resource_reservations`, `resource_reservation_tenant_index`, `resource_reservation_attempts`, `resource_hosts`, `resource_exclusivity`, `resource_fairness`, `resource_concurrency`, `resource_anti_affinity`, `resource_disk_policies`, `change_envelopes`, `content_versions`, `change_cursors`, `change_blobs`, `change_features`, `change_evidence`, `change_policies`, `change_lineage`, `raft_log`, `raft_meta`, `xshard_prepare`, `xshard_decision`, `matviews`, `plan_matviews`, `matview_operator_state`, `capacity_cells`, `capacity_leases`, `capacity_usage`, `capacity_idempotency`, `work_item_claim_capabilities`, `work_item_claim_capability_invocations`, `native_work_item_authority`, `development_lane_holds`, `development_lane_tenant_index`, `development_lane_lane_index`, `development_lane_repository_branch_index`, `development_lane_worktree_index`, `development_lane_work_item_index`, `development_lane_counters`, `development_lane_pressure_index`, `development_lane_policies`, `development_lane_invocations`, `encryption_canary`, `series_chunks`, `series_meta`, `series_projection_state`
* Removal step: stop the engine, move `graph-*.redb` aside (keep it until the restarted engine is confirmed healthy), and restart; a fresh store is created. Backup bundles taken before this release cannot restore this file.

## `GRAPH_SHARD_FORMAT_UPGRADE_REQUIRED`: graph shard before repository enrichment budgets and supersession

* Store file: `graph-*.redb`
* Refused generation: graph shard before repository enrichment budgets and supersession
* Data lost: its graph rows require the explicit offline enrichment layout upgrade before normal open, or re-ingestion after moving the file aside
* Owner tables of the refused generation: `nodes`, `edges`, `ledger`, `semantic_store`, `audit_chain`, `provenance_anchor_members`, `graph_meta`, `work_item_command_sequence`, `resource_reservations`, `resource_reservation_tenant_index`, `resource_reservation_attempts`, `resource_hosts`, `resource_exclusivity`, `resource_fairness`, `resource_concurrency`, `resource_anti_affinity`, `resource_disk_policies`, `change_envelopes`, `content_versions`, `change_cursors`, `change_blobs`, `change_features`, `change_evidence`, `change_policies`, `change_lineage`, `raft_log`, `raft_meta`, `xshard_prepare`, `xshard_decision`, `matviews`, `plan_matviews`, `matview_operator_state`, `capacity_cells`, `capacity_leases`, `capacity_usage`, `capacity_idempotency`, `work_item_claim_capabilities`, `work_item_claim_capability_invocations`, `native_work_item_authority`, `development_lane_holds`, `development_lane_tenant_index`, `development_lane_lane_index`, `development_lane_repository_branch_index`, `development_lane_worktree_index`, `development_lane_work_item_index`, `development_lane_counters`, `development_lane_pressure_index`, `development_lane_policies`, `development_lane_invocations`, `encryption_canary`, `series_chunks`, `series_meta`, `series_projection_state`, `storage_scrub_cursor`
* Removal step: none. This generation is upgraded in place with `epistemic-graph-server store-upgrade apply <data-dir> --confirm` while the engine is stopped; see the procedure below. Moving the file aside instead loses its rows.

## `GRAPH_SHARD_FORMAT_UPGRADE_REQUIRED`: graph shard before repository enrichment policy revisions

* Store file: `graph-*.redb`
* Refused generation: graph shard before repository enrichment policy revisions
* Data lost: its graph rows require re-ingestion after moving the file aside; the offline enrichment upgrade only accepts the exact pre-enrichment layout
* Owner tables of the refused generation: `nodes`, `edges`, `ledger`, `semantic_store`, `audit_chain`, `provenance_anchor_members`, `graph_meta`, `work_item_command_sequence`, `resource_reservations`, `resource_reservation_tenant_index`, `resource_reservation_attempts`, `resource_hosts`, `resource_exclusivity`, `resource_fairness`, `resource_concurrency`, `resource_anti_affinity`, `resource_disk_policies`, `change_envelopes`, `content_versions`, `change_cursors`, `change_blobs`, `change_features`, `change_evidence`, `change_policies`, `change_lineage`, `raft_log`, `raft_meta`, `xshard_prepare`, `xshard_decision`, `matviews`, `plan_matviews`, `matview_operator_state`, `capacity_cells`, `capacity_leases`, `capacity_usage`, `capacity_idempotency`, `repository_enrichment_budgets`, `repository_enrichment_supersessions`, `repository_enrichment_parks`, `work_item_claim_capabilities`, `work_item_claim_capability_invocations`, `native_work_item_authority`, `development_lane_holds`, `development_lane_tenant_index`, `development_lane_lane_index`, `development_lane_repository_branch_index`, `development_lane_worktree_index`, `development_lane_work_item_index`, `development_lane_counters`, `development_lane_pressure_index`, `development_lane_policies`, `development_lane_invocations`, `encryption_canary`, `series_chunks`, `series_meta`, `series_projection_state`, `storage_scrub_cursor`
* Removal step: stop the engine, move `graph-*.redb` aside (keep it until the restarted engine is confirmed healthy), and restart; a fresh store is created. Backup bundles taken before this release cannot restore this file.

## `GRAPH_SHARD_FORMAT_UPGRADE_REQUIRED`: graph shard before the operation audit-append idempotency index

* Store file: `graph-*.redb`
* Refused generation: graph shard before the operation audit-append idempotency index
* Data lost: its graph rows require the explicit offline audit-request-index layout upgrade before normal open, or re-ingestion after moving the file aside
* Owner tables of the refused generation: `nodes`, `edges`, `ledger`, `semantic_store`, `audit_chain`, `provenance_anchor_members`, `graph_meta`, `work_item_command_sequence`, `resource_reservations`, `resource_reservation_tenant_index`, `resource_reservation_attempts`, `resource_hosts`, `resource_exclusivity`, `resource_fairness`, `resource_concurrency`, `resource_anti_affinity`, `resource_disk_policies`, `change_envelopes`, `content_versions`, `change_cursors`, `change_blobs`, `change_features`, `change_evidence`, `change_policies`, `change_lineage`, `raft_log`, `raft_meta`, `xshard_prepare`, `xshard_decision`, `matviews`, `plan_matviews`, `matview_operator_state`, `capacity_cells`, `capacity_leases`, `capacity_usage`, `capacity_idempotency`, `repository_enrichment_budgets`, `repository_enrichment_policy_revisions`, `repository_enrichment_supersessions`, `repository_enrichment_parks`, `work_item_claim_capabilities`, `work_item_claim_capability_invocations`, `native_work_item_authority`, `development_lane_holds`, `development_lane_tenant_index`, `development_lane_lane_index`, `development_lane_repository_branch_index`, `development_lane_worktree_index`, `development_lane_work_item_index`, `development_lane_counters`, `development_lane_pressure_index`, `development_lane_policies`, `development_lane_invocations`, `encryption_canary`, `series_chunks`, `series_meta`, `series_projection_state`, `storage_scrub_cursor`
* Removal step: none. This generation is upgraded in place with `epistemic-graph-server store-upgrade apply <data-dir> --confirm` while the engine is stopped; see the procedure below. Moving the file aside instead loses its rows.

## `AGENT_LIBRARY_FORMAT_UPGRADE_REQUIRED`: Agent Library before connector packs and governed write-back

* Store file: `agent_library.redb`
* Refused generation: Agent Library before connector packs and governed write-back
* Data lost: its pre-ConnectorPack and governed-write-back Agent Library rows are intentionally not upgraded
* Owner tables of the refused generation: `agent_library`, `agent_library_heads`, `agent_graph`, `agent_graph_heads`, `agent_component`, `agent_component_heads`, `agent_template`, `agent_template_heads`
* Removal step: stop the engine, move `agent_library.redb` aside (keep it until the restarted engine is confirmed healthy), and restart; a fresh store is created. Backup bundles taken before this release cannot restore this file.

## `AGENT_LIBRARY_FORMAT_UPGRADE_REQUIRED`: Agent Library before served MCP catalog authority

* Store file: `agent_library.redb`
* Refused generation: Agent Library before served MCP catalog authority
* Data lost: its rows require the explicit offline MCP catalog layout upgrade before normal open, or republishing after moving the file aside
* Owner tables of the refused generation: `agent_library`, `agent_library_heads`, `agent_graph`, `agent_graph_heads`, `agent_component`, `agent_component_heads`, `agent_template`, `agent_template_heads`, `connector_pack_heads`, `connector_pack_members`, `connector_pack_imports`, `connector_pack_body_holders`, `connector_pack_bindings`, `decision_artifacts`, `decision_records`, `write_back_change_sets`, `write_back_idempotency`, `write_back_receipts`, `write_back_receipt_heads`
* Removal step: none. This generation is upgraded in place with `epistemic-graph-server store-upgrade apply <data-dir> --confirm` while the engine is stopped; see the procedure below. Moving the file aside instead loses its rows.

## Offline upgrade procedure

A predecessor listed in the table below is upgraded in place, with its rows preserved, by an explicit operator command. The engine never runs an upgrade itself: it refuses to start on such a store and names the command.

1. Stop the engine. Both verbs refuse while an engine holds the data directory.
2. Inspect: `epistemic-graph-server store-upgrade inspect <data-dir>`. This reads every store's manifest and changes nothing. Exit status `0` means nothing to do, `10` an upgrade is available, `20` a store is in a format this build neither opens nor upgrades, `30` a store can not be read, and `1` the command was refused.
3. Apply: `epistemic-graph-server store-upgrade apply <data-dir> --confirm`. Each upgrade is one atomic commit that creates the missing empty tables and replaces the owner manifest. Existing rows are not rewritten, and a failed or interrupted upgrade leaves the file in its previous layout. No separate copy of the store is kept, so take a filesystem snapshot first if one is wanted. The run stops at the first failure with exit status `1`. A store it does not upgrade is never written; a second run reports nothing to do. Before an upgrade is admitted the store is checked on a private scratch copy, which needs free space of about the store's size under `<data-dir>/.store-upgrade-staging` (`--staging-dir` names another local directory, which must have mode 0700 and sit inside a mode 0700 directory; `--max-store-bytes` sets the per-store budget).
4. Start the engine.

A store that was not closed cleanly, as after a killed engine, cannot be read without recovering it, which `inspect` never does: it reports the store as not inspectable (`30`). `apply` recovers a private copy of such a store, upgrades the store if a registered upgrade admits it, and otherwise leaves it untouched and reports `30`. The engine recovers that store at its next start and then opens it or refuses it by name.

The last output line of both verbs is one JSON object with the outcome, the exit status and one entry per store.

| Store | Predecessor generation | Store file |
|---|---|---|
| `sql` | SQL catalog store before durable source checkpoints | `sql.redb` |
| `sql` | SQL catalog store before durable ANN and edge index generations | `sql.redb` |
| `agent_library` | Agent Library before served MCP catalog authority | `agent_library.redb` |
| `graph_shard` | graph shard before repository enrichment budgets and supersession | `graph-*.redb` |
| `graph_shard` | graph shard before the operation audit-append idempotency index | `graph-*.redb` |
