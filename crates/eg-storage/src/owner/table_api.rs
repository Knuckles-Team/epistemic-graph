use crate::owner::domain::{
    BlobOwner, JobsOwner, KvOwner, OwnerDomain, RbacOwner, SemanticIndexOwner, StatechartOwner,
    TimeSeriesOwner,
};

/// Sealed typed table contract. External code can use declared row domains but
/// cannot manufacture a table declaration or substitute a redb codec.
pub trait OwnerTable<D: OwnerDomain>: sealed::Sealed {
    type Key;
    type Value;
    const TABLE_ID: &'static str;

    fn access_class() -> OwnerTableAccess {
        owner_table_access(Self::TABLE_ID)
    }
}

/// Closed cutover disposition for every owner table. `DomainService` tables
/// are intentionally unavailable as row CRUD and must be migrated behind the
/// owning crate's admitted atomic service operation in Phase 2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnerTableAccess {
    DomainService,
    SharedService,
}

mod sealed {
    pub trait Sealed {}
}

macro_rules! declared_owner_tables {
    ($($row:ident : $domain:ty => ($key:ty, $value:ty, $table:literal)),+ $(,)?) => {$ (
        pub struct $row;
        impl sealed::Sealed for $row {}
        impl OwnerTable<$domain> for $row {
            type Key = $key;
            type Value = $value;
            const TABLE_ID: &'static str = $table;
        }
    )+};
}

declared_owner_tables!(
    KvRows: KvOwner => ((String, String), Vec<u8>, "kv"),
    RbacRows: RbacOwner => (String, Vec<u8>, "rbac_v1"),
    AnalyticsJobsRows: JobsOwner => (String, Vec<u8>, "analytics_jobs"),
    AnalyticsCommittedRows: JobsOwner => (String, String, "analytics_job_committed_results"),
    JobIntentsRows: JobsOwner => (String, Vec<u8>, "job_intents"),
    JobIdempotencyRows: JobsOwner => (String, String, "job_idempotency_ledger"),
    AnalyticsKnowledgeRows: JobsOwner => (String, Vec<u8>, "analytics_job_knowledge_batches"),
    AnalyticsSchedulerRows: JobsOwner => (String, u64, "analytics_job_scheduler_meta"),
    JobReadyPriorityRows: JobsOwner => ((u32, i64, String), (), "analytics_job_ready_by_priority"),
    JobReadyCapabilityRows: JobsOwner => ((String, u32, i64, String), (), "analytics_job_ready_by_capability"),
    JobLeaseWorkerRows: JobsOwner => (String, String, "analytics_job_lease_by_worker"),
    JobLeaseExpiryRows: JobsOwner => ((i64, String), (), "analytics_job_lease_by_expiry"),
    JobActiveTotalsRows: JobsOwner => (String, (u64, u64), "analytics_job_active_totals_by_tenant"),
    JobDeadlineRows: JobsOwner => ((i64, String), (), "analytics_job_by_deadline"),
    JobCancellationRows: JobsOwner => (String, (), "analytics_job_cancellation_reconcile"),
    StatechartDefinitionRows: StatechartOwner => (String, Vec<u8>, "statechart_defs"),
    StatechartInstanceRows: StatechartOwner => (String, Vec<u8>, "statechart_instances"),
    SeriesChunkRows: TimeSeriesOwner => ((String, u64), Vec<u8>, "series_chunks"),
    SeriesMetadataRows: TimeSeriesOwner => (String, Vec<u8>, "series_meta"),
    SeriesProjectionRows: TimeSeriesOwner => (String, Vec<u8>, "series_projection_state"),
    BlobRows: BlobOwner => (String, Vec<u8>, "cas_blobs"),
    BlobUploadRows: BlobOwner => (u64, Vec<u8>, "cas_uploads"),
    SemanticBindingRows: SemanticIndexOwner => ((String, String, u64), Vec<u8>, "semantic_bindings_v1"),
    SemanticBindingHeadRows: SemanticIndexOwner => ((String, String), u64, "semantic_binding_heads_v1"),
    SemanticStageRows: SemanticIndexOwner => ((String, String, String), Vec<u8>, "semantic_stage_transitions_v1"),
    SemanticStateRows: SemanticIndexOwner => ((String, String), Vec<u8>, "semantic_binding_state_transitions_v1"),
    SemanticSourceProgressRows: SemanticIndexOwner => ((String, String, u64, String), Vec<u8>, "semantic_source_progress_v1"),
    SemanticActivePointerRows: SemanticIndexOwner => ((String, String), Vec<u8>, "semantic_active_pointers_v1"),
    SemanticDeadLetterRows: SemanticIndexOwner => ((String, String, u32), Vec<u8>, "semantic_dead_letters_v1"),
    SemanticTombstoneRows: SemanticIndexOwner => ((String, String, u64), Vec<u8>, "semantic_tombstones_v1"),
    SemanticSqlManifestRows: SemanticIndexOwner => ((String, String, u64, String), Vec<u8>, "semantic_sql_source_manifests_v1"),
    SemanticGraphManifestRows: SemanticIndexOwner => ((String, String, u64, String), Vec<u8>, "semantic_graph_projection_manifests_v1"),
    SemanticAuthorizationRows: SemanticIndexOwner => ((String, String, u64, String), Vec<u8>, "semantic_authorization_receipts_v1"),
    SemanticCheckpointRows: SemanticIndexOwner => ((String, String, u64, String), Vec<u8>, "semantic_generation_checkpoints_v1"),
    SemanticLexicalRows: SemanticIndexOwner => ((String, String, u64), Vec<u8>, "semantic_lexical_manifests_v1"),
    SemanticAnnRows: SemanticIndexOwner => ((String, String, u64), Vec<u8>, "semantic_ann_manifests_v1"),
    SemanticVectorRows: SemanticIndexOwner => ((String, String, u64, String), Vec<u8>, "semantic_vectors_v1"),
    AnnCodeRows: SemanticIndexOwner => ((String, String, u64, String), Vec<u8>, "eg_ann"),
);

/// Closed dispatch over the declared owner tables. The `unreachable!` arm is a
/// build-time invariant, not a runtime input path: the only caller is
/// `OwnerTable::access_class`, whose `TABLE_ID` comes from the sealed
/// `declared_owner_tables!` macro above, so no caller outside this file can
/// supply a name.
pub(crate) fn owner_table_access(table: &str) -> OwnerTableAccess {
    match table {
        "cas_chunks" | "cas_refcount" => OwnerTableAccess::SharedService,
        "kv"
        | "rbac_v1"
        | "analytics_jobs"
        | "analytics_job_committed_results"
        | "job_intents"
        | "job_idempotency_ledger"
        | "analytics_job_knowledge_batches"
        | "analytics_job_scheduler_meta"
        | "analytics_job_ready_by_priority"
        | "analytics_job_ready_by_capability"
        | "analytics_job_lease_by_worker"
        | "analytics_job_lease_by_expiry"
        | "analytics_job_active_totals_by_tenant"
        | "analytics_job_by_deadline"
        | "analytics_job_cancellation_reconcile"
        | "statechart_defs"
        | "statechart_instances"
        | "series_chunks"
        | "series_meta"
        | "series_projection_state"
        | "cas_blobs"
        | "cas_uploads"
        | "semantic_bindings_v1"
        | "semantic_binding_heads_v1"
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
        | "semantic_vectors_v1"
        | "eg_ann"
        | "eg_kvcache_cold"
        | "path_index_v1"
        | "verified_request_replay_v2"
        | "viz_provenance"
        | "cold_graphs"
        | "tenant_catalog"
        | "node_info"
        | "node_info_meta"
        | "cluster_hierarchy" => OwnerTableAccess::DomainService,
        name if name.starts_with("__sql_") => OwnerTableAccess::DomainService,
        // Every graph-shard table is reached through the shard's own admitted
        // owner write, never as a shared service: one shard file is one
        // physical authority serving the graphs bound to it.
        name if crate::owner::graph_shard::GRAPH_SHARD_TABLES.contains(&name) => {
            OwnerTableAccess::DomainService
        }
        _ => unreachable!("table outside closed owner access registry: {table}"),
    }
}
