//! The closed table contract every declared physical table carries.
//!
//! One entry per table name: its redb key/value types, its codecs, its
//! ownership and scope class, and its capability mask. The owner manifest is
//! the hash of exactly these contracts, so a divergent redeclaration anywhere
//! fails the manifest digest closed.

use crate::owner::graph_shard;
use crate::owner::layout::{OwnerLayout, OWNER_LAYOUT_DOMAINS};
use crate::owner::registry::owner_table_names;
use crate::physical::manifest::{TableContract, TableOwnership, TableScope};

pub(crate) const CAP_READ: u16 = 1;
pub(crate) const CAP_INSERT: u16 = 1 << 1;
pub(crate) const CAP_UPDATE: u16 = 1 << 2;
pub(crate) const CAP_DELETE: u16 = 1 << 3;
pub(crate) const CAP_CAS: u16 = 1 << 4;

pub(crate) fn expected_table_contracts(layout: OwnerLayout) -> Vec<TableContract> {
    crate::tables::ledger_table_names()
        .into_iter()
        .map(|name| table_contract(name, None))
        .chain(
            owner_table_names(layout)
                .iter()
                .map(|name| table_contract(name, Some(layout))),
        )
        .collect()
}

/// Contract for one ledger table, which no owner layout owns.
pub(crate) fn ledger_table_contract(name: &str) -> TableContract {
    table_contract(name, None)
}

/// Contract for one owner table under the layout that declares it.
pub(crate) fn expected_owner_table_contract(name: &str, layout: OwnerLayout) -> TableContract {
    table_contract(name, Some(layout))
}

pub(crate) fn table_contract(name: &str, owner: Option<OwnerLayout>) -> TableContract {
    let ownership = if owner.is_some() {
        TableOwnership::Owner
    } else {
        TableOwnership::Ledger
    };
    let index = matches!(
        name,
        "mutation_outbox_topic_index"
            | "analytics_job_ready_by_priority"
            | "analytics_job_ready_by_capability"
            | "analytics_job_scheduler_meta"
            | "analytics_job_lease_by_worker"
            | "analytics_job_lease_by_expiry"
            | "analytics_job_active_totals_by_tenant"
            | "analytics_job_by_deadline"
            | "analytics_job_cancellation_reconcile"
            | "semantic_binding_heads"
            | "semantic_lexical_manifests"
            | "semantic_ann_manifests"
            | "semantic_vectors"
    ) || (owner == Some(OwnerLayout::GraphShard)
        && graph_shard::is_derived_index(name));
    let shared = matches!(name, "cas_chunks" | "cas_refcount");
    let key_type_id = key_type_id(name);
    let value_type_id = value_type_id(name);
    TableContract {
        table_id: name.to_string(),
        schema_id: format!("eg.redb.{name}.v1"),
        key_type_id: key_type_id.to_string(),
        value_type_id: value_type_id.to_string(),
        key_codec: "redb-native-key-v1".to_string(),
        value_codec: "redb-native-value-v1".to_string(),
        logical_schema_id: format!("eg.logical.{name}.v1"),
        logical_codec_id: logical_codec_id(name).to_string(),
        ownership,
        domain: owner.map(|layout| OWNER_LAYOUT_DOMAINS[layout as usize]),
        scope: if shared {
            TableScope::SharedService
        } else if let Some(scope) = shard_scope(name, owner) {
            scope
        } else if matches!(
            owner,
            Some(
                OwnerLayout::Rbac
                    | OwnerLayout::Jobs
                    | OwnerLayout::Statechart
                    | OwnerLayout::Kv
                    | OwnerLayout::PathIndex
                    | OwnerLayout::Blob
                    | OwnerLayout::RequestReplay
                    | OwnerLayout::VizProvenance
                    | OwnerLayout::ColdTier
                    | OwnerLayout::TenantCatalog
                    | OwnerLayout::NodeInfo
                    | OwnerLayout::ClusterHierarchy
                    | OwnerLayout::AgentLibrary
            )
        ) {
            TableScope::StorePrivate
        } else if owner.is_some()
            || !matches!(name, "mutation_store_root" | "mutation_owner_manifest")
        {
            TableScope::Serving
        } else {
            TableScope::Physical
        },
        capabilities: table_capabilities(name),
        index,
        derived: index,
    }
}

/// Every `unreachable!` below dispatches over the **statically closed** table
/// census: the only callers are `expected_table_contracts` and
/// `validate_owner_registry_equality`, both of which iterate
/// `crate::tables::ledger_table_names()` and `owner_table_names(layout)`, and
/// `validate_owner_registry_equality` fails a test the moment a declared table
/// has no contract entry. A name reaching one of these arms would mean the
/// registry and the contract table had already diverged, which is a build-time
/// invariant break rather than a runtime input.
fn key_type_id(name: &str) -> &'static str {
    ledger_key_type(name)
        .or_else(|| owner_key_type(name))
        .or_else(|| semantic_key_type(name))
        .or_else(|| sql_key_type(name))
        .or_else(|| graph_shard::key_type(name))
        .unwrap_or_else(|| unreachable!("table outside closed owner manifest: {name}"))
}

fn ledger_key_type(name: &str) -> Option<&'static str> {
    match name {
        "mutation_outbox_topic_index" => Some("(&str,&str,u64,u64,&str,u32)"),
        "mutation_outbox_deliveries" => Some("(&str,&str,&str,u32)"),
        "ledger_outbox" => Some("(&str,&str,u32)"),
        "ledger_batches"
        | "ledger_maintenance"
        | "ledger_private_payloads"
        | "mutation_outbox_consumers"
        | "mutation_outbox_cursors"
        | "mutation_outbox_claim_cursors"
        | "mutation_outbox_fairness"
        | "mutation_replay_nonces"
        | "mutation_replay_operations"
        | "mutation_classes" => Some("(&str,&str)"),
        "mutation_store_root"
        | "mutation_scope_bindings"
        | "mutation_owner_manifest"
        | "ledger_versions"
        | "ledger_fences" => Some("&str"),
        _ => None,
    }
}

fn owner_key_type(name: &str) -> Option<&'static str> {
    jobs_key_type(name)
        .or_else(|| connector_owner_key_type(name))
        .or_else(|| domain_owner_key_type(name))
}

fn jobs_key_type(name: &str) -> Option<&'static str> {
    match name {
        "analytics_job_ready_by_capability" => Some("(&str,u32,i64,&str)"),
        "analytics_job_ready_by_priority" => Some("(u32,i64,&str)"),
        "analytics_job_lease_by_expiry" | "analytics_job_by_deadline" => Some("(i64,&str)"),
        "analytics_jobs"
        | "analytics_job_committed_results"
        | "job_intents"
        | "job_idempotency_ledger"
        | "analytics_job_knowledge_batches"
        | "analytics_job_scheduler_meta"
        | "analytics_job_lease_by_worker"
        | "analytics_job_active_totals_by_tenant"
        | "analytics_job_cancellation_reconcile" => Some("&str"),
        _ => None,
    }
}

fn domain_owner_key_type(name: &str) -> Option<&'static str> {
    match name {
        "series_chunks" => Some("(&str,u64)"),
        "kv" => Some("(&str,&str)"),
        "cas_uploads" | "node_info" => Some("u64"),
        // One arm, not four: every RF-ADR-008 layer's head table is keyed the
        // same `(tenant, id)` way, and four identical arms were four branches
        // saying one thing.
        // `cas_holders` shares the shape: one row per (blob digest, holder).
        "agent_library_heads"
        | "agent_graph_heads"
        | "agent_component_heads"
        | "agent_template_heads"
        | "cas_holders" => Some("(&str,&str)"),
        "eg_kvcache_cold" => Some("&[u8]"),
        "cas_chunks" | "cas_refcount" | "cas_blobs" | "cas_retention" | "cas_counters" => {
            Some("&str")
        }
        "rbac"
        | "path_index"
        | "statechart_defs"
        | "statechart_instances"
        | "series_meta"
        | "series_projection_state"
        | "verified_request_replay"
        | "viz_provenance"
        | "cold_graphs"
        | "tenant_catalog"
        | "node_info_meta"
        | "cluster_hierarchy" => Some("&str"),
        // Likewise: every layer's revision table is keyed `(tenant, id,
        // revision)`, which is what makes a retained revision addressable
        // after the head moves past it.
        "agent_library" | "agent_graph" | "agent_component" | "agent_template" => {
            Some("(&str,&str,u64)")
        }
        _ => None,
    }
}

/// Key shapes for the connector-pack and governed write-back owner tables.
fn connector_owner_key_type(name: &str) -> Option<&'static str> {
    match name {
        "connector_pack_heads"
        | "connector_pack_bindings"
        | "decision_records"
        | "write_back_change_sets"
        | "write_back_idempotency"
        | "write_back_receipt_heads" => Some("(&str,&str)"),
        "connector_pack_members" => Some("(&str,&str,&str)"),
        "connector_pack_imports" | "write_back_receipts" => Some("(&str,&str,u64)"),
        "connector_pack_body_holders" => Some("(&str,&str,&str,u64)"),
        _ => None,
    }
}

/// The SQL catalog/row store owned by `eg-query`.
fn sql_key_type(name: &str) -> Option<&'static str> {
    match name {
        "__sql_rows__" | "__sql_schema_catalog_order__" => Some("(&str,u64)"),
        "__sql_schema_migrations__" | "__sql_source_checkpoints__" => Some("(&str,&str,&str)"),
        "__sql_schema_migration_order__" => Some("(&str,&str,u64)"),
        "__sql_schema_versions__" => Some("(&str,&str)"),
        name if name.starts_with("__sql_") => Some("&str"),
        _ => None,
    }
}

fn semantic_key_type(name: &str) -> Option<&'static str> {
    match name {
        "semantic_source_progress"
        | "semantic_sql_source_manifests"
        | "semantic_graph_projection_manifests"
        | "semantic_authorization_receipts"
        | "semantic_generation_checkpoints"
        | "semantic_vectors"
        // `eg_ann` is keyed `(tenant, binding, generation, part)` like the rest
        // of the generation-scoped semantic payload, not by a flat name.
        | "eg_ann" => Some("(&str,&str,u64,&str)"),
        "semantic_generation_checkpoint_heads" => Some("(&str,&str,u64,&str,&str)"),
        "semantic_stage_transitions" => Some("(&str,&str,&str)"),
        "semantic_dead_letters" => Some("(&str,&str,&str,u32)"),
        "semantic_bindings"
        | "semantic_tombstones"
        | "semantic_lexical_manifests"
        | "semantic_ann_manifests" => Some("(&str,&str,u64)"),
        "semantic_binding_heads"
        | "semantic_binding_state_transitions"
        | "semantic_active_pointers" => Some("(&str,&str)"),
        _ => None,
    }
}
fn value_type_id(name: &str) -> &'static str {
    if let Some(value) = sql_value_type(name) {
        return value;
    }
    match name {
        "ledger_versions"
        | "analytics_job_scheduler_meta"
        | "cas_refcount"
        | "cas_retention"
        | "cas_counters"
        | "verified_request_replay"
        | "semantic_binding_heads" => "u64",
        "agent_library_heads"
        | "agent_graph_heads"
        | "agent_component_heads"
        | "agent_template_heads"
        | "write_back_receipt_heads" => "u64",
        "analytics_job_active_totals_by_tenant" => "(u64,u64)",
        "mutation_outbox_topic_index"
        | "analytics_job_ready_by_priority"
        | "analytics_job_ready_by_capability"
        | "analytics_job_lease_by_expiry"
        | "analytics_job_by_deadline"
        | "analytics_job_cancellation_reconcile" => "()",
        "ledger_maintenance"
        | "mutation_outbox_consumers"
        | "mutation_replay_nonces"
        | "analytics_job_committed_results"
        | "job_idempotency_ledger"
        | "analytics_job_lease_by_worker" => "&str",
        "mutation_store_root"
        | "mutation_scope_bindings"
        | "mutation_owner_manifest"
        | "ledger_batches"
        | "ledger_fences"
        | "ledger_outbox"
        | "ledger_private_payloads"
        | "mutation_outbox_deliveries"
        | "mutation_outbox_cursors"
        | "mutation_outbox_claim_cursors"
        | "mutation_outbox_fairness"
        | "mutation_replay_operations"
        | "mutation_classes"
        | "rbac"
        | "analytics_jobs"
        | "job_intents"
        | "analytics_job_knowledge_batches"
        | "statechart_defs"
        | "statechart_instances"
        | "series_chunks"
        | "series_meta"
        | "series_projection_state"
        | "kv"
        | "cas_chunks"
        | "cas_blobs"
        | "cas_uploads"
        | "cas_holders"
        | "semantic_bindings"
        | "semantic_stage_transitions"
        | "semantic_binding_state_transitions"
        | "semantic_source_progress"
        | "semantic_active_pointers"
        | "semantic_dead_letters"
        | "semantic_tombstones"
        | "semantic_sql_source_manifests"
        | "semantic_graph_projection_manifests"
        | "semantic_authorization_receipts"
        | "semantic_generation_checkpoints"
        | "semantic_generation_checkpoint_heads"
        | "semantic_lexical_manifests"
        | "semantic_ann_manifests"
        | "semantic_vectors"
        | "path_index"
        | "eg_ann"
        | "eg_kvcache_cold"
        | "viz_provenance"
        | "cold_graphs"
        | "tenant_catalog"
        | "node_info"
        | "node_info_meta"
        | "cluster_hierarchy"
        | "agent_library"
        | "agent_graph"
        | "agent_component"
        | "agent_template"
        | "connector_pack_heads"
        | "connector_pack_members"
        | "connector_pack_imports"
        | "connector_pack_body_holders"
        | "connector_pack_bindings"
        | "decision_records"
        | "write_back_change_sets"
        | "write_back_idempotency"
        | "write_back_receipts" => "&[u8]",
        name => graph_shard::value_type(name)
            .unwrap_or_else(|| unreachable!("table outside closed owner manifest: {name}")),
    }
}

fn sql_value_type(name: &str) -> Option<&'static str> {
    match name {
        "__sql_seq__"
        | "__sql_schema_catalog_versions__"
        | "__sql_schema_versions__"
        | "__sql_property_graph_seq__" => Some("u64"),
        "__sql_views__"
        | "__sql_extensions__"
        | "__sql_schema_migration_order__"
        | "__sql_schema_catalog_order__" => Some("&str"),
        name if name.starts_with("__sql_") => Some("&[u8]"),
        _ => None,
    }
}

fn logical_codec_id(name: &str) -> &'static str {
    if name.starts_with("__sql_") {
        return match sql_value_type(name) {
            Some("u64") | Some("&str") => "redb-scalar-v1",
            _ => "msgpack-v1",
        };
    }
    ledger_and_job_codec(name)
        .or_else(|| semantic_codec(name))
        .or_else(|| mutation_family_codec(name))
        .unwrap_or_else(|| {
            graph_shard::logical_codec(name)
                .unwrap_or_else(|| unreachable!("table outside closed owner manifest: {name}"))
        })
}

/// `logical_codec_id`'s ledger/analytics-job/misc-scalar tables -- matches
/// over table names (`&str`, not a closed enum), so the fallthrough below is
/// a real "not this group" result, not a discarded default.
fn ledger_and_job_codec(name: &str) -> Option<&'static str> {
    Some(match name {
        "ledger_private_payloads" => "authenticated-sealed-bytes-v1",
        "eg_ann" | "eg_kvcache_cold" | "cold_graphs" => "raw-bytes-v1",
        "path_index"
        | "viz_provenance"
        | "tenant_catalog"
        | "node_info"
        | "node_info_meta"
        | "cluster_hierarchy"
        | "agent_library"
        | "agent_graph"
        | "agent_component"
        | "agent_template"
        | "connector_pack_heads"
        | "connector_pack_members"
        | "connector_pack_imports"
        | "connector_pack_body_holders"
        | "connector_pack_bindings"
        | "write_back_change_sets"
        | "write_back_idempotency"
        | "write_back_receipts" => "msgpack-v1",
        "decision_records" => "json-utf8-v1",
        "rbac" => "json-utf8-v1",
        "kv" | "cas_chunks" => "raw-bytes-v1",
        "series_chunks" => "packed-timeseries-chunk-v1",
        "ledger_maintenance"
        | "ledger_versions"
        | "mutation_outbox_topic_index"
        | "mutation_outbox_consumers"
        | "mutation_replay_nonces"
        | "analytics_job_committed_results"
        | "job_idempotency_ledger"
        | "analytics_job_scheduler_meta"
        | "analytics_job_ready_by_priority"
        | "analytics_job_ready_by_capability"
        | "analytics_job_lease_by_worker"
        | "analytics_job_lease_by_expiry"
        | "analytics_job_active_totals_by_tenant"
        | "analytics_job_by_deadline"
        | "analytics_job_cancellation_reconcile"
        | "cas_refcount"
        | "cas_retention"
        | "cas_counters"
        | "verified_request_replay"
        | "semantic_binding_heads"
        | "agent_library_heads"
        | "agent_graph_heads"
        | "agent_component_heads"
        | "agent_template_heads"
        | "write_back_receipt_heads" => "redb-scalar-v1",
        _ => return None,
    })
}

/// `logical_codec_id`'s semantic-index tables.
fn semantic_codec(name: &str) -> Option<&'static str> {
    Some(match name {
        "semantic_bindings"
        | "semantic_stage_transitions"
        | "semantic_binding_state_transitions"
        | "semantic_source_progress"
        | "semantic_active_pointers"
        | "semantic_dead_letters"
        | "semantic_tombstones"
        | "semantic_sql_source_manifests"
        | "semantic_graph_projection_manifests"
        | "semantic_authorization_receipts"
        | "semantic_generation_checkpoints"
        | "semantic_generation_checkpoint_heads"
        | "semantic_lexical_manifests"
        | "semantic_ann_manifests"
        | "semantic_vectors" => "semantic-index-bytes-v1",
        _ => return None,
    })
}

/// Every name in `names` maps to `codec`, nothing else does. A shared shape
/// for the several `logical_codec_id` helpers whose whole table collapses to
/// one output literal (`mutation_family_codec` below), so growing one of
/// them doesn't reduce to the same "match a set, else None" control flow
/// `semantic_codec` already has under a different name (dupehound).
fn one_codec_for(name: &str, names: &[&str], codec: &'static str) -> Option<&'static str> {
    names.contains(&name).then_some(codec)
}

/// `logical_codec_id`'s mutation-store/outbox/statechart/series family.
fn mutation_family_codec(name: &str) -> Option<&'static str> {
    one_codec_for(
        name,
        &[
            "mutation_store_root",
            "mutation_scope_bindings",
            "mutation_owner_manifest",
            "ledger_batches",
            "ledger_fences",
            "ledger_outbox",
            "mutation_outbox_deliveries",
            "mutation_outbox_cursors",
            "mutation_outbox_claim_cursors",
            "mutation_outbox_fairness",
            "mutation_replay_operations",
            "mutation_classes",
            "analytics_jobs",
            "job_intents",
            "analytics_job_knowledge_batches",
            "statechart_defs",
            "statechart_instances",
            "series_meta",
            "series_projection_state",
            "cas_blobs",
            "cas_uploads",
            "cas_holders",
        ],
        "msgpack-v1",
    )
}

fn table_capabilities(name: &str) -> u16 {
    if name.starts_with("__sql_") {
        return CAP_READ | CAP_INSERT | CAP_UPDATE | CAP_DELETE;
    }
    ledger_and_job_capabilities(name)
        .or_else(|| index_and_scalar_capabilities(name))
        .or_else(|| mutation_and_semantic_capabilities(name))
        .or_else(|| agent_capabilities(name))
        .unwrap_or_else(|| {
            graph_shard::capabilities(name)
                .unwrap_or_else(|| unreachable!("table outside closed owner manifest: {name}"))
        })
}

/// `table_capabilities`'s ledger/analytics-job core tables -- matches over
/// table names (`&str`, not a closed enum), so the fallthrough below is a
/// real "not this group" result, not a discarded default.
fn ledger_and_job_capabilities(name: &str) -> Option<u16> {
    Some(match name {
        "mutation_store_root" | "mutation_owner_manifest" => CAP_READ | CAP_INSERT | CAP_UPDATE,
        "ledger_maintenance"
        | "ledger_outbox"
        | "analytics_job_committed_results"
        | "job_idempotency_ledger"
        | "analytics_job_knowledge_batches"
        | "statechart_defs"
        | "viz_provenance" => CAP_READ | CAP_INSERT,
        "cas_chunks" | "cas_blobs" => CAP_READ | CAP_INSERT | CAP_DELETE,
        "rbac"
        | "analytics_jobs"
        | "job_intents"
        | "analytics_job_scheduler_meta"
        | "statechart_instances" => CAP_READ | CAP_INSERT | CAP_UPDATE,
        _ => return None,
    })
}

/// `table_capabilities`'s index/scalar (CAS-capable and plain) tables.
fn index_and_scalar_capabilities(name: &str) -> Option<u16> {
    Some(match name {
        "ledger_private_payloads"
        | "mutation_outbox_topic_index"
        | "analytics_job_ready_by_priority"
        | "analytics_job_ready_by_capability"
        | "analytics_job_lease_by_worker"
        | "analytics_job_lease_by_expiry"
        | "analytics_job_by_deadline"
        | "analytics_job_cancellation_reconcile"
        | "semantic_binding_heads"
        | "semantic_lexical_manifests"
        | "semantic_ann_manifests"
        | "semantic_vectors"
        | "verified_request_replay" => CAP_READ | CAP_INSERT | CAP_DELETE,
        "ledger_versions" | "ledger_fences" | "kv" | "cas_refcount" => {
            CAP_READ | CAP_INSERT | CAP_UPDATE | CAP_DELETE | CAP_CAS
        }
        "path_index"
        | "eg_ann"
        | "eg_kvcache_cold"
        | "analytics_job_active_totals_by_tenant"
        | "series_chunks"
        | "series_meta"
        | "series_projection_state"
        | "cold_graphs"
        | "tenant_catalog"
        | "node_info"
        | "node_info_meta"
        | "cluster_hierarchy"
        | "cas_uploads"
        | "cas_holders"
        | "cas_retention"
        | "cas_counters" => CAP_READ | CAP_INSERT | CAP_UPDATE | CAP_DELETE,
        _ => return None,
    })
}

/// `table_capabilities`'s mutation-outbox/semantic-pipeline family.
fn mutation_and_semantic_capabilities(name: &str) -> Option<u16> {
    Some(match name {
        "mutation_scope_bindings"
        | "ledger_batches"
        | "mutation_outbox_consumers"
        | "mutation_outbox_deliveries"
        | "mutation_outbox_cursors"
        | "mutation_outbox_claim_cursors"
        | "mutation_outbox_fairness"
        | "mutation_replay_nonces"
        | "mutation_replay_operations"
        | "mutation_classes"
        | "semantic_bindings"
        | "semantic_stage_transitions"
        | "semantic_binding_state_transitions"
        | "semantic_source_progress"
        | "semantic_active_pointers"
        | "semantic_dead_letters"
        | "semantic_tombstones"
        | "semantic_sql_source_manifests"
        | "semantic_graph_projection_manifests"
        | "semantic_authorization_receipts"
        | "semantic_generation_checkpoints"
        | "semantic_generation_checkpoint_heads" => CAP_READ | CAP_INSERT | CAP_UPDATE | CAP_DELETE,
        _ => return None,
    })
}

/// `table_capabilities`'s append-only agent-library family.
fn agent_capabilities(name: &str) -> Option<u16> {
    Some(match name {
        // Append-only: a retained revision row is never updated or deleted,
        // which is what makes a tombstone a later revision rather than an edit.
        "agent_library" | "agent_graph" | "agent_component" | "agent_template" => {
            CAP_READ | CAP_INSERT
        }
        "agent_library_heads"
        | "agent_graph_heads"
        | "agent_component_heads"
        | "agent_template_heads" => CAP_READ | CAP_INSERT | CAP_UPDATE,
        _ => return connector_capabilities(name),
    })
}

fn connector_capabilities(name: &str) -> Option<u16> {
    Some(match name {
        "connector_pack_imports"
        | "connector_pack_body_holders"
        | "decision_records"
        | "write_back_change_sets"
        | "write_back_idempotency"
        | "write_back_receipts" => CAP_READ | CAP_INSERT,
        "connector_pack_heads" | "connector_pack_members" | "write_back_receipt_heads" => {
            CAP_READ | CAP_INSERT | CAP_UPDATE
        }
        "connector_pack_bindings" => CAP_READ | CAP_INSERT | CAP_UPDATE | CAP_DELETE,
        _ => return None,
    })
}

/// The graph shard classifies its tables' scope per table rather than per
/// layout: most keys lead with the graph name (the serving scope), while the
/// Raft, cross-shard, matview, canary and series rows belong to the file.
fn shard_scope(name: &str, owner: Option<OwnerLayout>) -> Option<TableScope> {
    if owner != Some(OwnerLayout::GraphShard) {
        return None;
    }
    graph_shard::scope(name)
}
