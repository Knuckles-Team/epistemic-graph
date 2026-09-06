use crate::owner::contract::expected_owner_table_contract;
use crate::owner::layout::OwnerLayout;
use crate::physical::root::is_retired_prototype_table;
use crate::recovery::evidence::{copy_table, HashSnapshot, StrictTableEvidence};
use crate::tables::open_declared_ledger_tables;
use redb::{Key, ReadTransaction, TableDefinition, TableHandle, Value, WriteTransaction};
use sha2::Sha256;

// Closed owner-table registry. These names and types are the manifest contract.
const RBAC: TableDefinition<'static, &str, &[u8]> = TableDefinition::new("rbac_v1");
const JOB_TABLES_BYTES: [TableDefinition<'static, &str, &[u8]>; 3] = [
    TableDefinition::new("analytics_jobs"),
    TableDefinition::new("job_intents"),
    TableDefinition::new("analytics_job_knowledge_batches"),
];
const JOB_TABLES_STR: [TableDefinition<'static, &str, &str>; 3] = [
    TableDefinition::new("analytics_job_committed_results"),
    TableDefinition::new("job_idempotency_ledger"),
    TableDefinition::new("analytics_job_lease_by_worker"),
];
const JOB_META: TableDefinition<'static, &str, u64> =
    TableDefinition::new("analytics_job_scheduler_meta");
const READY_PRIORITY: TableDefinition<'static, (u32, i64, &str), ()> =
    TableDefinition::new("analytics_job_ready_by_priority");
const READY_CAPABILITY: TableDefinition<'static, (&str, u32, i64, &str), ()> =
    TableDefinition::new("analytics_job_ready_by_capability");
const LEASE_EXPIRY: TableDefinition<'static, (i64, &str), ()> =
    TableDefinition::new("analytics_job_lease_by_expiry");
const ACTIVE_TOTALS: TableDefinition<'static, &str, (u64, u64)> =
    TableDefinition::new("analytics_job_active_totals_by_tenant");
const BY_DEADLINE: TableDefinition<'static, (i64, &str), ()> =
    TableDefinition::new("analytics_job_by_deadline");
const CANCELLATION: TableDefinition<'static, &str, ()> =
    TableDefinition::new("analytics_job_cancellation_reconcile");
const STATECHARTS: [TableDefinition<'static, &str, &[u8]>; 2] = [
    TableDefinition::new("statechart_defs"),
    TableDefinition::new("statechart_instances"),
];
const TS_CHUNKS: TableDefinition<'static, (&str, u64), &[u8]> =
    TableDefinition::new("series_chunks");
const TS_ROWS: [TableDefinition<'static, &str, &[u8]>; 2] = [
    TableDefinition::new("series_meta"),
    TableDefinition::new("series_projection_state"),
];
pub(crate) const KV: TableDefinition<'static, (&str, &str), &[u8]> = TableDefinition::new("kv");
const KV_COLD: TableDefinition<'static, &[u8], &[u8]> = TableDefinition::new("eg_kvcache_cold");
const PATH_INDEX: TableDefinition<'static, &str, &[u8]> = TableDefinition::new("path_index_v1");
/// The ANN code buffers of one semantic index generation.
///
/// Keyed `(tenant, binding, generation, part)` -- the generation component is
/// what lets two generations of one binding coexist in the file while `N+1` is
/// built and `N` is still serving. A flat key could not: the previous durable
/// form wrote the fixed names `meta`/`codes`/`refine`, so a second generation
/// overwrote the serving one in place. The `part` component bounds a single
/// stored value, so a code buffer is chunked rather than written as one
/// multi-hundred-megabyte row.
///
/// Public because it is the one declaration of this table: the semantic domain
/// addresses it through the layout-bounded owner write/read handles, and a
/// second hand-written `TableDefinition` in a consumer crate is exactly the
/// drift `validate_owner_registry_equality` exists to refuse.
pub const ANN_CODES: TableDefinition<'static, (&str, &str, u64, &str), &[u8]> =
    TableDefinition::new("eg_ann");
// The SQL catalog/row store owned by `eg-query`. Declared here because
// RF-RULING-004 puts the complete physical table registry in the storage
// kernel: a consumer crate that declares its own tables is a second physical
// authority.
const SQL_STR_BYTES: [TableDefinition<'static, &str, &[u8]>; 7] = [
    TableDefinition::new("__sql_catalog__"),
    TableDefinition::new("__sql_functions__"),
    TableDefinition::new("__sql_ann_indexes__"),
    TableDefinition::new("__sql_secondary_indexes__"),
    TableDefinition::new("__sql_secondary_index_entries__"),
    TableDefinition::new("__sql_hypertables__"),
    TableDefinition::new("__sql_mutation_batches__"),
];
const SQL_STR_STR: [TableDefinition<'static, &str, &str>; 2] = [
    TableDefinition::new("__sql_views__"),
    TableDefinition::new("__sql_extensions__"),
];
const SQL_ROWS: TableDefinition<'static, (&str, u64), &[u8]> =
    TableDefinition::new("__sql_rows__");
const SQL_SEQ: TableDefinition<'static, &str, u64> = TableDefinition::new("__sql_seq__");
const SQL_CATALOG_VERSIONS: TableDefinition<'static, &str, u64> =
    TableDefinition::new("__sql_schema_catalog_versions__");
const SQL_MUTATION_IDEMPOTENCY: TableDefinition<'static, (&str, &str, &str), &str> =
    TableDefinition::new("__sql_mutation_idempotency__");
const SQL_MUTATION_VERSION: TableDefinition<'static, (&str, &str), u64> =
    TableDefinition::new("__sql_mutation_version__");
const SQL_MUTATION_FENCE: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("__sql_mutation_fence__");
const SQL_MUTATION_OUTBOX: TableDefinition<'static, (&str, u32), &[u8]> =
    TableDefinition::new("__sql_mutation_outbox__");
const SQL_SCHEMA_VERSIONS: TableDefinition<'static, (&str, &str), u64> =
    TableDefinition::new("__sql_schema_versions__");
const SQL_SCHEMA_MIGRATIONS: TableDefinition<'static, (&str, &str, &str), &[u8]> =
    TableDefinition::new("__sql_schema_migrations__");
const SQL_SCHEMA_MIGRATION_ORDER: TableDefinition<'static, (&str, &str, u64), &str> =
    TableDefinition::new("__sql_schema_migration_order__");
const SQL_SCHEMA_CATALOG_ORDER: TableDefinition<'static, (&str, u64), &str> =
    TableDefinition::new("__sql_schema_catalog_order__");
const BLOB_CHUNKS: TableDefinition<'static, &str, &[u8]> = TableDefinition::new("cas_chunks");
const BLOB_OBJECTS: TableDefinition<'static, &str, &[u8]> = TableDefinition::new("cas_blobs");
const BLOB_REFS: TableDefinition<'static, &str, u64> = TableDefinition::new("cas_refcount");
const BLOB_UPLOADS: TableDefinition<'static, u64, &[u8]> = TableDefinition::new("cas_uploads");
const SEMANTIC_BINDINGS: TableDefinition<'static, (&str, &str, u64), &[u8]> =
    TableDefinition::new("semantic_bindings_v1");
const SEMANTIC_HEADS: TableDefinition<'static, (&str, &str), u64> =
    TableDefinition::new("semantic_binding_heads_v1");
const SEMANTIC_STAGES: TableDefinition<'static, (&str, &str, &str), &[u8]> =
    TableDefinition::new("semantic_stage_transitions_v1");
/// The binding's durable authority record (model identity and dimensions),
/// keyed `(tenant, binding)` -- one per binding, not per generation.
///
/// Public because the semantic domain addresses it through the layout-bounded
/// owner handles; a second hand-written declaration in a consumer crate is the
/// drift `validate_owner_registry_equality` exists to refuse.
pub const SEMANTIC_STATES: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("semantic_binding_state_transitions_v1");
const SEMANTIC_SOURCE_PROGRESS: TableDefinition<'static, (&str, &str, u64, &str), &[u8]> =
    TableDefinition::new("semantic_source_progress_v1");
/// The binding's live-generation pointer, keyed `(tenant, binding)`. One row,
/// so two live generations of one binding are structurally impossible.
///
/// Public because the semantic domain addresses it through the layout-bounded
/// owner handles; a second hand-written declaration in a consumer crate is the
/// drift `validate_owner_registry_equality` exists to refuse.
pub const SEMANTIC_POINTERS: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("semantic_active_pointers_v1");
const SEMANTIC_DEAD_LETTERS: TableDefinition<'static, (&str, &str, u32), &[u8]> =
    TableDefinition::new("semantic_dead_letters_v1");
const SEMANTIC_TOMBSTONES: TableDefinition<'static, (&str, &str, u64), &[u8]> =
    TableDefinition::new("semantic_tombstones_v1");
const SEMANTIC_SQL_SOURCES: TableDefinition<'static, (&str, &str, u64, &str), &[u8]> =
    TableDefinition::new("semantic_sql_source_manifests_v1");
const SEMANTIC_GRAPH_PROJECTIONS: TableDefinition<'static, (&str, &str, u64, &str), &[u8]> =
    TableDefinition::new("semantic_graph_projection_manifests_v1");
const SEMANTIC_AUTH_RECEIPTS: TableDefinition<'static, (&str, &str, u64, &str), &[u8]> =
    TableDefinition::new("semantic_authorization_receipts_v1");
const SEMANTIC_CHECKPOINTS: TableDefinition<'static, (&str, &str, u64, &str), &[u8]> =
    TableDefinition::new("semantic_generation_checkpoints_v1");
const SEMANTIC_LEXICAL: TableDefinition<'static, (&str, &str, u64), &[u8]> =
    TableDefinition::new("semantic_lexical_manifests_v1");
const SEMANTIC_ANN: TableDefinition<'static, (&str, &str, u64), &[u8]> =
    TableDefinition::new("semantic_ann_manifests_v1");
const SEMANTIC_VECTORS: TableDefinition<'static, (&str, &str, u64, &str), &[u8]> =
    TableDefinition::new("semantic_vectors_v1");
// Root-binary sidecar owner files. Each is one physical file with one fixed
// native `ControlPlane` serving scope, so each declares its own layout rather
// than sharing one: the census is exact, and a shared layout would force every
// one of these files to carry every other's table.
const REQUEST_REPLAY: TableDefinition<'static, &str, u64> =
    TableDefinition::new("verified_request_replay_v2");
const VIZ_PROVENANCE: TableDefinition<'static, &str, &[u8]> =
    TableDefinition::new("viz_provenance");
const COLD_GRAPHS: TableDefinition<'static, &str, &[u8]> = TableDefinition::new("cold_graphs");
const TENANT_CATALOG: TableDefinition<'static, &str, &[u8]> =
    TableDefinition::new("tenant_catalog");
const NODE_INFO: TableDefinition<'static, u64, &[u8]> = TableDefinition::new("node_info");
const NODE_INFO_META: TableDefinition<'static, &str, &[u8]> =
    TableDefinition::new("node_info_meta");
const CLUSTER_HIERARCHY: TableDefinition<'static, &str, &[u8]> =
    TableDefinition::new("cluster_hierarchy");

macro_rules! visit_owner_tables {
    ($layout:expr, $visit:ident) => {{
        match $layout {
            OwnerLayout::LedgerOnly => {}
            OwnerLayout::Rbac => $visit!(RBAC),
            OwnerLayout::Jobs => {
                for table in JOB_TABLES_BYTES {
                    $visit!(table);
                }
                for table in JOB_TABLES_STR {
                    $visit!(table);
                }
                $visit!(JOB_META);
                $visit!(READY_PRIORITY);
                $visit!(READY_CAPABILITY);
                $visit!(LEASE_EXPIRY);
                $visit!(ACTIVE_TOTALS);
                $visit!(BY_DEADLINE);
                $visit!(CANCELLATION);
            }
            OwnerLayout::Statechart => {
                for table in STATECHARTS {
                    $visit!(table);
                }
            }
            OwnerLayout::TimeSeries => {
                $visit!(TS_CHUNKS);
                for table in TS_ROWS {
                    $visit!(table);
                }
            }
            OwnerLayout::Kv => {
                $visit!(KV);
                $visit!(KV_COLD);
            }
            OwnerLayout::PathIndex => $visit!(PATH_INDEX),
            OwnerLayout::Sql => {
                for table in SQL_STR_BYTES {
                    $visit!(table);
                }
                for table in SQL_STR_STR {
                    $visit!(table);
                }
                $visit!(SQL_ROWS);
                $visit!(SQL_SEQ);
                $visit!(SQL_CATALOG_VERSIONS);
                $visit!(SQL_MUTATION_IDEMPOTENCY);
                $visit!(SQL_MUTATION_VERSION);
                $visit!(SQL_MUTATION_FENCE);
                $visit!(SQL_MUTATION_OUTBOX);
                $visit!(SQL_SCHEMA_VERSIONS);
                $visit!(SQL_SCHEMA_MIGRATIONS);
                $visit!(SQL_SCHEMA_MIGRATION_ORDER);
                $visit!(SQL_SCHEMA_CATALOG_ORDER);
            }
            OwnerLayout::Blob => {
                $visit!(BLOB_CHUNKS);
                $visit!(BLOB_OBJECTS);
                $visit!(BLOB_REFS);
                $visit!(BLOB_UPLOADS);
            }
            OwnerLayout::SemanticIndex => {
                $visit!(SEMANTIC_BINDINGS);
                $visit!(SEMANTIC_HEADS);
                $visit!(SEMANTIC_STAGES);
                $visit!(SEMANTIC_STATES);
                $visit!(SEMANTIC_SOURCE_PROGRESS);
                $visit!(SEMANTIC_POINTERS);
                $visit!(SEMANTIC_DEAD_LETTERS);
                $visit!(SEMANTIC_TOMBSTONES);
                $visit!(SEMANTIC_SQL_SOURCES);
                $visit!(SEMANTIC_GRAPH_PROJECTIONS);
                $visit!(SEMANTIC_AUTH_RECEIPTS);
                $visit!(SEMANTIC_CHECKPOINTS);
                $visit!(SEMANTIC_LEXICAL);
                $visit!(SEMANTIC_ANN);
                $visit!(SEMANTIC_VECTORS);
                $visit!(ANN_CODES);
            }
            OwnerLayout::RequestReplay => $visit!(REQUEST_REPLAY),
            OwnerLayout::VizProvenance => $visit!(VIZ_PROVENANCE),
            OwnerLayout::ColdTier => $visit!(COLD_GRAPHS),
            OwnerLayout::TenantCatalog => $visit!(TENANT_CATALOG),
            OwnerLayout::NodeInfo => {
                $visit!(NODE_INFO);
                $visit!(NODE_INFO_META);
            }
            OwnerLayout::ClusterHierarchy => $visit!(CLUSTER_HIERARCHY),
        }
    }};
}

pub(crate) fn open_declared_owner_tables(
    wtx: &WriteTransaction,
    layout: OwnerLayout,
) -> Result<(), String> {
    validate_owner_registry_equality()?;
    macro_rules! open {
        ($table:expr) => {{
            open_table(wtx, $table)?;
        }};
    }
    visit_owner_tables!(layout, open);
    Ok(())
}

pub(crate) fn validate_declared_owner_tables(
    rtx: &ReadTransaction,
    layout: OwnerLayout,
) -> Result<(), String> {
    validate_owner_registry_equality()?;
    macro_rules! validate {
        ($table:expr) => {{
            validate_table(rtx, $table)?;
        }};
    }
    visit_owner_tables!(layout, validate);
    validate_table_census(
        rtx.list_tables().map_err(|error| error.to_string())?,
        rtx.list_multimap_tables()
            .map_err(|error| error.to_string())?,
        layout,
    )
}

/// Validate the complete closed declaration before opening any table through a
/// write transaction. This ordering prevents a missing table from being
/// recreated as an accidental repair path by `WriteTransaction::open_table`.
pub(crate) fn validate_declared_tables_write(
    wtx: &WriteTransaction,
    layout: OwnerLayout,
) -> Result<(), String> {
    validate_table_census(
        wtx.list_tables().map_err(|error| error.to_string())?,
        wtx.list_multimap_tables()
            .map_err(|error| error.to_string())?,
        layout,
    )?;
    open_declared_ledger_tables(wtx)?;
    open_declared_owner_tables(wtx, layout)
}

fn validate_table_census<N, M>(normal: N, multimap: M, layout: OwnerLayout) -> Result<(), String>
where
    N: IntoIterator,
    N::Item: redb::TableHandle,
    M: IntoIterator,
    M::Item: redb::MultimapTableHandle,
{
    let expected = crate::tables::ledger_table_names()
        .into_iter()
        .chain(owner_table_names(layout).iter().copied())
        .map(str::to_string)
        .collect::<std::collections::BTreeSet<_>>();
    let actual = normal
        .into_iter()
        .map(|table| table.name().to_string())
        .collect::<std::collections::BTreeSet<_>>();
    if actual != expected {
        return Err("mutation store table census differs from its closed manifest".to_string());
    }
    if multimap.into_iter().next().is_some() {
        return Err("mutation store contains an undeclared multimap table".to_string());
    }
    Ok(())
}


pub(crate) fn copy_declared_owner_tables(
    source: &ReadTransaction,
    target: &WriteTransaction,
    layout: OwnerLayout,
) -> Result<u64, String> {
    let mut rows = 0;
    macro_rules! copy {
        ($table:expr) => {{
            rows += copy_table(source, target, $table)?;
        }};
    }
    visit_owner_tables!(layout, copy);
    Ok(rows)
}

pub(crate) fn hash_declared_owner_tables(
    source: HashSnapshot<'_>,
    layout: OwnerLayout,
    hasher: &mut Sha256,
    tables: &mut Vec<StrictTableEvidence>,
) -> Result<u64, String> {
    let mut rows = 0;
    macro_rules! hash {
        ($table:expr) => {{
            let (count, fingerprint) = source.hash_table(hasher, $table)?;
            rows += count;
            tables.push(StrictTableEvidence {
                table_id: $table.name().to_string(),
                rows: count,
                fingerprint,
            });
        }};
    }
    visit_owner_tables!(layout, hash);
    Ok(rows)
}

/// Exact sorted physical table names for a closed owner layout.
///
/// Snapshot providers use this projection to bind their static section
/// manifest without duplicating the mutation-store registry.
pub fn declared_table_names(layout: OwnerLayout) -> Vec<&'static str> {
    let mut names = crate::tables::ledger_table_names();
    names.extend_from_slice(owner_table_names(layout));
    names.sort_unstable();
    names
}

/// Exactly the owner tables one layout declares -- the ledger and the three
/// physical-identity tables are never in this set.
///
/// Public because the mutation kernel needs it to bound an owner-row write to
/// its own layout (`AdmittedOwnerWrite::open_table`).
pub fn owner_table_names(layout: OwnerLayout) -> &'static [&'static str] {
    match layout {
        OwnerLayout::LedgerOnly => &[],
        OwnerLayout::Rbac => &["rbac_v1"],
        OwnerLayout::Jobs => &[
            "analytics_jobs",
            "analytics_job_committed_results",
            "job_intents",
            "job_idempotency_ledger",
            "analytics_job_knowledge_batches",
            "analytics_job_scheduler_meta",
            "analytics_job_ready_by_priority",
            "analytics_job_ready_by_capability",
            "analytics_job_lease_by_worker",
            "analytics_job_lease_by_expiry",
            "analytics_job_active_totals_by_tenant",
            "analytics_job_by_deadline",
            "analytics_job_cancellation_reconcile",
        ],
        OwnerLayout::Statechart => &["statechart_defs", "statechart_instances"],
        OwnerLayout::TimeSeries => &["series_chunks", "series_meta", "series_projection_state"],
        OwnerLayout::Kv => &["kv", "eg_kvcache_cold"],
        OwnerLayout::PathIndex => &["path_index_v1"],
        OwnerLayout::Sql => &[
            "__sql_catalog__",
            "__sql_functions__",
            "__sql_ann_indexes__",
            "__sql_secondary_indexes__",
            "__sql_secondary_index_entries__",
            "__sql_hypertables__",
            "__sql_mutation_batches__",
            "__sql_views__",
            "__sql_extensions__",
            "__sql_rows__",
            "__sql_seq__",
            "__sql_schema_catalog_versions__",
            "__sql_mutation_idempotency__",
            "__sql_mutation_version__",
            "__sql_mutation_fence__",
            "__sql_mutation_outbox__",
            "__sql_schema_versions__",
            "__sql_schema_migrations__",
            "__sql_schema_migration_order__",
            "__sql_schema_catalog_order__",
        ],
        OwnerLayout::Blob => &["cas_chunks", "cas_blobs", "cas_refcount", "cas_uploads"],
        OwnerLayout::SemanticIndex => &[
            "semantic_bindings_v1",
            "semantic_binding_heads_v1",
            "semantic_stage_transitions_v1",
            "semantic_binding_state_transitions_v1",
            "semantic_source_progress_v1",
            "semantic_active_pointers_v1",
            "semantic_dead_letters_v1",
            "semantic_tombstones_v1",
            "semantic_sql_source_manifests_v1",
            "semantic_graph_projection_manifests_v1",
            "semantic_authorization_receipts_v1",
            "semantic_generation_checkpoints_v1",
            "semantic_lexical_manifests_v1",
            "semantic_ann_manifests_v1",
            "semantic_vectors_v1",
            "eg_ann",
        ],
        OwnerLayout::RequestReplay => &["verified_request_replay_v2"],
        OwnerLayout::VizProvenance => &["viz_provenance"],
        OwnerLayout::ColdTier => &["cold_graphs"],
        OwnerLayout::TenantCatalog => &["tenant_catalog"],
        OwnerLayout::NodeInfo => &["node_info", "node_info_meta"],
        OwnerLayout::ClusterHierarchy => &["cluster_hierarchy"],
    }
}

pub(crate) fn is_mutation_authority_marker(name: &str) -> bool {
    is_known_mutation_table(name) || is_retired_prototype_table(name)
}

pub(crate) fn is_known_mutation_table(name: &str) -> bool {
    crate::tables::ledger_table_names().contains(&name)
        || owner_layouts()
            .iter()
            .any(|layout| owner_table_names(*layout).contains(&name))
}

fn validate_owner_registry_equality() -> Result<(), String> {
    validate_ledger_registry_types()?;
    for layout in owner_layouts() {
        let mut declared = Vec::new();
        macro_rules! record {
            ($table:expr) => {{
                validate_registry_type(layout, $table)?;
                declared.push($table.name().to_string());
            }};
        }
        visit_owner_tables!(layout, record);
        let declared = declared
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        let canonical = owner_table_names(layout)
            .iter()
            .copied()
            .map(str::to_string)
            .collect::<std::collections::BTreeSet<_>>();
        if declared != canonical {
            return Err("owner table definitions differ from the canonical registry".to_string());
        }
    }
    Ok(())
}

/// The ledger tables get the same K/V-vs-contract cross-check the owner tables
/// already had. Without it, a wrong `key_type_id`/`value_type_id` string in
/// `contract.rs` silently shifts the layout digest instead of failing.
pub(crate) fn validate_ledger_registry_types() -> Result<(), String> {
    macro_rules! check {
        ($table:expr) => {{
            validate_ledger_type($table)?;
        }};
    }
    crate::tables::visit_ledger_tables!(check);
    Ok(())
}

fn validate_ledger_type<K, V>(table: TableDefinition<'static, K, V>) -> Result<(), String>
where
    K: Key + 'static,
    V: Value + 'static,
{
    let contract = crate::owner::contract::ledger_table_contract(table.name());
    if contract.key_type_id != K::type_name().name()
        || contract.value_type_id != V::type_name().name()
    {
        return Err(format!(
            "ledger table {} Redb type differs from its manifest contract",
            table.name()
        ));
    }
    Ok(())
}

fn validate_registry_type<K, V>(
    layout: OwnerLayout,
    table: TableDefinition<'static, K, V>,
) -> Result<(), String>
where
    K: Key + 'static,
    V: Value + 'static,
{
    let contract = expected_owner_table_contract(table.name(), layout);
    if contract.key_type_id != K::type_name().name()
        || contract.value_type_id != V::type_name().name()
    {
        return Err("owner table Redb type differs from its manifest contract".to_string());
    }
    Ok(())
}

pub(crate) fn owner_layouts() -> [OwnerLayout; 16] {
    [
        OwnerLayout::LedgerOnly,
        OwnerLayout::Rbac,
        OwnerLayout::Jobs,
        OwnerLayout::Statechart,
        OwnerLayout::TimeSeries,
        OwnerLayout::Kv,
        OwnerLayout::Blob,
        OwnerLayout::SemanticIndex,
        OwnerLayout::Sql,
        OwnerLayout::PathIndex,
        OwnerLayout::RequestReplay,
        OwnerLayout::VizProvenance,
        OwnerLayout::ColdTier,
        OwnerLayout::TenantCatalog,
        OwnerLayout::NodeInfo,
        OwnerLayout::ClusterHierarchy,
    ]
}


fn open_table<K, V>(
    wtx: &WriteTransaction,
    table: TableDefinition<'static, K, V>,
) -> Result<(), String>
where
    K: Key + 'static,
    V: Value + 'static,
{
    wtx.open_table(table)
        .map(|_| ())
        .map_err(|error| error.to_string())
}


fn validate_table<K, V>(
    rtx: &ReadTransaction,
    table: TableDefinition<'static, K, V>,
) -> Result<(), String>
where
    K: Key + 'static,
    V: Value + 'static,
{
    rtx.open_table(table)
        .map(|_| ())
        .map_err(|error| error.to_string())
}
