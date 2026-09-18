use crate::owner::domain::{
    AgentLibraryOwner, BlobOwner, JobsOwner, KvOwner, OwnerDomain, RbacOwner, SemanticIndexOwner,
    StatechartOwner, TimeSeriesOwner,
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
    RbacRows: RbacOwner => (String, Vec<u8>, "rbac"),
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
    BlobHolderRows: BlobOwner => ((String, String), Vec<u8>, "cas_holders"),
    BlobRetentionRows: BlobOwner => (String, u64, "cas_retention"),
    BlobCounterRows: BlobOwner => (String, u64, "cas_counters"),
    SemanticBindingRows: SemanticIndexOwner => ((String, String, u64), Vec<u8>, "semantic_bindings"),
    SemanticBindingHeadRows: SemanticIndexOwner => ((String, String), u64, "semantic_binding_heads"),
    SemanticStageRows: SemanticIndexOwner => ((String, String, String), Vec<u8>, "semantic_stage_transitions"),
    SemanticStateRows: SemanticIndexOwner => ((String, String), Vec<u8>, "semantic_binding_state_transitions"),
    SemanticSourceProgressRows: SemanticIndexOwner => ((String, String, u64, String), Vec<u8>, "semantic_source_progress"),
    SemanticActivePointerRows: SemanticIndexOwner => ((String, String), Vec<u8>, "semantic_active_pointers"),
    SemanticDeadLetterRows: SemanticIndexOwner => ((String, String, String, u32), Vec<u8>, "semantic_dead_letters"),
    SemanticTombstoneRows: SemanticIndexOwner => ((String, String, u64), Vec<u8>, "semantic_tombstones"),
    SemanticSqlManifestRows: SemanticIndexOwner => ((String, String, u64, String), Vec<u8>, "semantic_sql_source_manifests"),
    SemanticGraphManifestRows: SemanticIndexOwner => ((String, String, u64, String), Vec<u8>, "semantic_graph_projection_manifests"),
    SemanticAuthorizationRows: SemanticIndexOwner => ((String, String, u64, String), Vec<u8>, "semantic_authorization_receipts"),
    SemanticCheckpointRows: SemanticIndexOwner => ((String, String, u64, String), Vec<u8>, "semantic_generation_checkpoints"),
    SemanticCheckpointHeadRows: SemanticIndexOwner => ((String, String, u64, String, String), Vec<u8>, "semantic_generation_checkpoint_heads"),
    SemanticLexicalRows: SemanticIndexOwner => ((String, String, u64), Vec<u8>, "semantic_lexical_manifests"),
    SemanticAnnRows: SemanticIndexOwner => ((String, String, u64), Vec<u8>, "semantic_ann_manifests"),
    SemanticVectorRows: SemanticIndexOwner => ((String, String, u64, String), Vec<u8>, "semantic_vectors"),
    AnnCodeRows: SemanticIndexOwner => ((String, String, u64, String), Vec<u8>, "eg_ann"),
    AgentLibraryRevisionRows: AgentLibraryOwner => ((String, String, u64), Vec<u8>, "agent_library"),
    AgentLibraryHeadRows: AgentLibraryOwner => ((String, String), u64, "agent_library_heads"),
    // The other three RF-ADR-008 layers share the AgentLibrary owner file, so
    // they are declared here for the same reason the first one is: a table
    // that exists in `OwnerLayout::AgentLibrary` but not in this registry has
    // no cutover disposition, and `owner_table_access` has nothing to answer
    // with but `unreachable!()`.
    AgentGraphRevisionRows: AgentLibraryOwner => ((String, String, u64), Vec<u8>, "agent_graph"),
    AgentGraphHeadRows: AgentLibraryOwner => ((String, String), u64, "agent_graph_heads"),
    AgentComponentRevisionRows: AgentLibraryOwner => ((String, String, u64), Vec<u8>, "agent_component"),
    AgentComponentHeadRows: AgentLibraryOwner => ((String, String), u64, "agent_component_heads"),
    AgentTemplateRevisionRows: AgentLibraryOwner => ((String, String, u64), Vec<u8>, "agent_template"),
    AgentTemplateHeadRows: AgentLibraryOwner => ((String, String), u64, "agent_template_heads"),
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
        | "rbac"
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
        | "cas_holders"
        | "cas_retention"
        | "cas_counters"
        | "semantic_bindings"
        | "semantic_binding_heads"
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
        | "eg_ann"
        | "eg_kvcache_cold"
        | "path_index"
        | "verified_request_replay"
        | "viz_provenance"
        | "cold_graphs"
        | "tenant_catalog"
        | "node_info"
        | "node_info_meta"
        | "cluster_hierarchy"
        | "agent_library"
        | "agent_library_heads"
        | "agent_graph"
        | "agent_graph_heads"
        | "agent_component"
        | "agent_component_heads"
        | "agent_template"
        | "agent_template_heads" => OwnerTableAccess::DomainService,
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
