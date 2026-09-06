//! The closed table contract every declared physical table carries.
//!
//! One entry per table name: its redb key/value types, its codecs, its
//! ownership and scope class, and its capability mask. The owner manifest is
//! the hash of exactly these contracts, so a divergent redeclaration anywhere
//! fails the manifest digest closed.

use crate::owner::layout::{OwnerLayout, LEDGER_TABLE_NAMES, OWNER_LAYOUT_DOMAINS};
use crate::owner::registry::owner_table_names;
use crate::physical::manifest::{TableContract, TableOwnership, TableScope};

pub(crate) const CAP_READ: u16 = 1;
pub(crate) const CAP_INSERT: u16 = 1 << 1;
pub(crate) const CAP_UPDATE: u16 = 1 << 2;
pub(crate) const CAP_DELETE: u16 = 1 << 3;
pub(crate) const CAP_CAS: u16 = 1 << 4;

pub(crate) fn expected_table_contracts(layout: OwnerLayout) -> Vec<TableContract> {
    LEDGER_TABLE_NAMES
        .iter()
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
        "mutation_outbox_topic_index_v1"
            | "analytics_job_ready_by_priority"
            | "analytics_job_ready_by_capability"
            | "analytics_job_scheduler_meta"
            | "analytics_job_lease_by_worker"
            | "analytics_job_lease_by_expiry"
            | "analytics_job_active_totals_by_tenant"
            | "analytics_job_by_deadline"
            | "analytics_job_cancellation_reconcile"
            | "semantic_binding_heads_v1"
            | "semantic_lexical_manifests_v1"
            | "semantic_ann_manifests_v1"
            | "semantic_vectors_v1"
    );
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
        } else if matches!(
            owner,
            Some(OwnerLayout::Rbac | OwnerLayout::Jobs | OwnerLayout::Statechart | OwnerLayout::Kv)
        ) {
            TableScope::StorePrivate
        } else if owner.is_some()
            || !matches!(
                name,
                "mutation_store_root_v1" | "mutation_owner_manifest_v1"
            )
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

fn key_type_id(name: &str) -> &'static str {
    ledger_key_type(name)
        .or_else(|| owner_key_type(name))
        .or_else(|| semantic_key_type(name))
        .unwrap_or_else(|| unreachable!("table outside closed owner manifest: {name}"))
}

fn ledger_key_type(name: &str) -> Option<&'static str> {
    match name {
        "mutation_outbox_topic_index_v1" => Some("(&str,&str,u64,u64,&str,u32)"),
        "mutation_outbox_deliveries_v1" => Some("(&str,&str,&str,u32)"),
        "mutation_outbox_v1" => Some("(&str,&str,u32)"),
        "mutation_batches_v1"
        | "mutation_idempotency_v1"
        | "mutation_private_payloads_v1"
        | "mutation_outbox_consumers_v1"
        | "mutation_outbox_cursors_v1"
        | "mutation_outbox_claim_cursors_v1"
        | "mutation_outbox_fairness_v1"
        | "mutation_replay_nonces_v1"
        | "mutation_replay_operations_v1" => Some("(&str,&str)"),
        "mutation_store_root_v1"
        | "mutation_scope_bindings_v1"
        | "mutation_owner_manifest_v1"
        | "mutation_versions_v1"
        | "mutation_fences_v1" => Some("&str"),
        _ => None,
    }
}

fn owner_key_type(name: &str) -> Option<&'static str> {
    jobs_key_type(name).or_else(|| domain_owner_key_type(name))
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
        "kv" | "cas_blobs" => Some("(&str,&str)"),
        "cas_uploads" => Some("(&str,u64)"),
        "cas_chunks" | "cas_refcount" => Some("&str"),
        "rbac_v1"
        | "statechart_defs"
        | "statechart_instances"
        | "series_meta"
        | "series_projection_state" => Some("&str"),
        _ => None,
    }
}

fn semantic_key_type(name: &str) -> Option<&'static str> {
    match name {
        "semantic_source_progress_v1"
        | "semantic_sql_source_manifests_v1"
        | "semantic_graph_projection_manifests_v1"
        | "semantic_authorization_receipts_v1"
        | "semantic_generation_checkpoints_v1"
        | "semantic_vectors_v1" => Some("(&str,&str,u64,&str)"),
        "semantic_stage_transitions_v1" => Some("(&str,&str,&str)"),
        "semantic_dead_letters_v1" => Some("(&str,&str,u32)"),
        "semantic_bindings_v1"
        | "semantic_tombstones_v1"
        | "semantic_lexical_manifests_v1"
        | "semantic_ann_manifests_v1" => Some("(&str,&str,u64)"),
        "semantic_binding_heads_v1"
        | "semantic_binding_state_transitions_v1"
        | "semantic_active_pointers_v1" => Some("(&str,&str)"),
        _ => None,
    }
}
fn value_type_id(name: &str) -> &'static str {
    match name {
        "mutation_versions_v1"
        | "analytics_job_scheduler_meta"
        | "cas_refcount"
        | "semantic_binding_heads_v1" => "u64",
        "analytics_job_active_totals_by_tenant" => "(u64,u64)",
        "mutation_outbox_topic_index_v1"
        | "analytics_job_ready_by_priority"
        | "analytics_job_ready_by_capability"
        | "analytics_job_lease_by_expiry"
        | "analytics_job_by_deadline"
        | "analytics_job_cancellation_reconcile" => "()",
        "mutation_idempotency_v1"
        | "mutation_outbox_consumers_v1"
        | "mutation_replay_nonces_v1"
        | "analytics_job_committed_results"
        | "job_idempotency_ledger"
        | "analytics_job_lease_by_worker" => "&str",
        "mutation_store_root_v1"
        | "mutation_scope_bindings_v1"
        | "mutation_owner_manifest_v1"
        | "mutation_batches_v1"
        | "mutation_fences_v1"
        | "mutation_outbox_v1"
        | "mutation_private_payloads_v1"
        | "mutation_outbox_deliveries_v1"
        | "mutation_outbox_cursors_v1"
        | "mutation_outbox_claim_cursors_v1"
        | "mutation_outbox_fairness_v1"
        | "mutation_replay_operations_v1"
        | "rbac_v1"
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
        | "semantic_bindings_v1"
        | "semantic_stage_transitions_v1"
        | "semantic_binding_state_transitions_v1"
        | "semantic_source_progress_v1"
        | "semantic_active_pointers_v1"
        | "semantic_dead_letters_v1"
        | "semantic_tombstones_v1"
        | "semantic_sql_source_manifests_v1"
        | "semantic_graph_projection_manifests_v1"
        | "semantic_authorization_receipts_v1"
        | "semantic_generation_checkpoints_v1"
        | "semantic_lexical_manifests_v1"
        | "semantic_ann_manifests_v1"
        | "semantic_vectors_v1" => "&[u8]",
        _ => unreachable!("table outside closed owner manifest: {name}"),
    }
}

fn logical_codec_id(name: &str) -> &'static str {
    match name {
        "mutation_private_payloads_v1" => "authenticated-sealed-bytes-v1",
        "rbac_v1" => "json-utf8-v1",
        "kv" | "cas_chunks" => "raw-bytes-v1",
        "series_chunks" => "packed-timeseries-chunk-v1",
        "mutation_idempotency_v1"
        | "mutation_versions_v1"
        | "mutation_outbox_topic_index_v1"
        | "mutation_outbox_consumers_v1"
        | "mutation_replay_nonces_v1"
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
        | "semantic_binding_heads_v1" => "redb-scalar-v1",
        "semantic_bindings_v1"
        | "semantic_stage_transitions_v1"
        | "semantic_binding_state_transitions_v1"
        | "semantic_source_progress_v1"
        | "semantic_active_pointers_v1"
        | "semantic_dead_letters_v1"
        | "semantic_tombstones_v1"
        | "semantic_sql_source_manifests_v1"
        | "semantic_graph_projection_manifests_v1"
        | "semantic_authorization_receipts_v1"
        | "semantic_generation_checkpoints_v1"
        | "semantic_lexical_manifests_v1"
        | "semantic_ann_manifests_v1"
        | "semantic_vectors_v1" => "semantic-index-bytes-v1",
        "mutation_store_root_v1"
        | "mutation_scope_bindings_v1"
        | "mutation_owner_manifest_v1"
        | "mutation_batches_v1"
        | "mutation_fences_v1"
        | "mutation_outbox_v1"
        | "mutation_outbox_deliveries_v1"
        | "mutation_outbox_cursors_v1"
        | "mutation_outbox_claim_cursors_v1"
        | "mutation_outbox_fairness_v1"
        | "mutation_replay_operations_v1"
        | "analytics_jobs"
        | "job_intents"
        | "analytics_job_knowledge_batches"
        | "statechart_defs"
        | "statechart_instances"
        | "series_meta"
        | "series_projection_state"
        | "cas_blobs"
        | "cas_uploads" => "msgpack-v1",
        _ => unreachable!("table outside closed owner manifest: {name}"),
    }
}

fn table_capabilities(name: &str) -> u16 {
    match name {
        "mutation_store_root_v1" | "mutation_owner_manifest_v1" => {
            CAP_READ | CAP_INSERT | CAP_UPDATE
        }
        "mutation_idempotency_v1"
        | "mutation_outbox_v1"
        | "analytics_job_committed_results"
        | "job_idempotency_ledger"
        | "analytics_job_knowledge_batches"
        | "statechart_defs" => CAP_READ | CAP_INSERT,
        "cas_chunks" | "cas_blobs" => CAP_READ | CAP_INSERT | CAP_DELETE,
        "rbac_v1"
        | "analytics_jobs"
        | "job_intents"
        | "analytics_job_scheduler_meta"
        | "statechart_instances" => CAP_READ | CAP_INSERT | CAP_UPDATE,
        "mutation_private_payloads_v1"
        | "mutation_outbox_topic_index_v1"
        | "analytics_job_ready_by_priority"
        | "analytics_job_ready_by_capability"
        | "analytics_job_lease_by_worker"
        | "analytics_job_lease_by_expiry"
        | "analytics_job_by_deadline"
        | "analytics_job_cancellation_reconcile"
        | "semantic_binding_heads_v1"
        | "semantic_lexical_manifests_v1"
        | "semantic_ann_manifests_v1"
        | "semantic_vectors_v1" => CAP_READ | CAP_INSERT | CAP_DELETE,
        "mutation_versions_v1" | "mutation_fences_v1" | "kv" | "cas_refcount" => {
            CAP_READ | CAP_INSERT | CAP_UPDATE | CAP_DELETE | CAP_CAS
        }
        "analytics_job_active_totals_by_tenant"
        | "series_chunks"
        | "series_meta"
        | "series_projection_state"
        | "cas_uploads" => CAP_READ | CAP_INSERT | CAP_UPDATE | CAP_DELETE,
        "mutation_scope_bindings_v1"
        | "mutation_batches_v1"
        | "mutation_outbox_consumers_v1"
        | "mutation_outbox_deliveries_v1"
        | "mutation_outbox_cursors_v1"
        | "mutation_outbox_claim_cursors_v1"
        | "mutation_outbox_fairness_v1"
        | "mutation_replay_nonces_v1"
        | "mutation_replay_operations_v1"
        | "semantic_bindings_v1"
        | "semantic_stage_transitions_v1"
        | "semantic_binding_state_transitions_v1"
        | "semantic_source_progress_v1"
        | "semantic_active_pointers_v1"
        | "semantic_dead_letters_v1"
        | "semantic_tombstones_v1"
        | "semantic_sql_source_manifests_v1"
        | "semantic_graph_projection_manifests_v1"
        | "semantic_authorization_receipts_v1"
        | "semantic_generation_checkpoints_v1" => CAP_READ | CAP_INSERT | CAP_UPDATE | CAP_DELETE,
        _ => unreachable!("table outside closed owner manifest: {name}"),
    }
}
