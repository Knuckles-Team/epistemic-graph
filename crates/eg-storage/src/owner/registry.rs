use crate::owner::contract::expected_owner_table_contract;
use crate::owner::layout::{OwnerLayout, LEDGER_TABLE_NAMES};
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
const BLOB_CHUNKS: TableDefinition<'static, &str, &[u8]> = TableDefinition::new("cas_chunks");
const BLOB_OBJECTS: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("cas_blobs");
const BLOB_REFS: TableDefinition<'static, &str, u64> = TableDefinition::new("cas_refcount");
const BLOB_UPLOADS: TableDefinition<'static, (&str, u64), &[u8]> =
    TableDefinition::new("cas_uploads");
const SEMANTIC_BINDINGS: TableDefinition<'static, (&str, &str, u64), &[u8]> =
    TableDefinition::new("semantic_bindings_v1");
const SEMANTIC_HEADS: TableDefinition<'static, (&str, &str), u64> =
    TableDefinition::new("semantic_binding_heads_v1");
const SEMANTIC_STAGES: TableDefinition<'static, (&str, &str, &str), &[u8]> =
    TableDefinition::new("semantic_stage_transitions_v1");
const SEMANTIC_STATES: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("semantic_binding_state_transitions_v1");
const SEMANTIC_SOURCE_PROGRESS: TableDefinition<'static, (&str, &str, u64, &str), &[u8]> =
    TableDefinition::new("semantic_source_progress_v1");
const SEMANTIC_POINTERS: TableDefinition<'static, (&str, &str), &[u8]> =
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

macro_rules! for_each_semantic_table {
    ($visit:ident, $($argument:expr),+) => {{
        $visit($($argument,)* SEMANTIC_BINDINGS)?;
        $visit($($argument,)* SEMANTIC_HEADS)?;
        $visit($($argument,)* SEMANTIC_STAGES)?;
        $visit($($argument,)* SEMANTIC_STATES)?;
        $visit($($argument,)* SEMANTIC_SOURCE_PROGRESS)?;
        $visit($($argument,)* SEMANTIC_POINTERS)?;
        $visit($($argument,)* SEMANTIC_DEAD_LETTERS)?;
        $visit($($argument,)* SEMANTIC_TOMBSTONES)?;
        $visit($($argument,)* SEMANTIC_SQL_SOURCES)?;
        $visit($($argument,)* SEMANTIC_GRAPH_PROJECTIONS)?;
        $visit($($argument,)* SEMANTIC_AUTH_RECEIPTS)?;
        $visit($($argument,)* SEMANTIC_CHECKPOINTS)?;
        $visit($($argument,)* SEMANTIC_LEXICAL)?;
        $visit($($argument,)* SEMANTIC_ANN)?;
        $visit($($argument,)* SEMANTIC_VECTORS)
    }};
}

pub(crate) fn open_declared_owner_tables(
    wtx: &WriteTransaction,
    layout: OwnerLayout,
) -> Result<(), String> {
    validate_owner_registry_equality()?;
    match layout {
        OwnerLayout::LedgerOnly => Ok(()),
        OwnerLayout::Rbac => open_table(wtx, RBAC),
        OwnerLayout::Jobs => {
            open_each(wtx, &JOB_TABLES_BYTES)?;
            open_each(wtx, &JOB_TABLES_STR)?;
            open_table(wtx, JOB_META)?;
            open_table(wtx, READY_PRIORITY)?;
            open_table(wtx, READY_CAPABILITY)?;
            open_table(wtx, LEASE_EXPIRY)?;
            open_table(wtx, ACTIVE_TOTALS)?;
            open_table(wtx, BY_DEADLINE)?;
            open_table(wtx, CANCELLATION)
        }
        OwnerLayout::Statechart => open_each(wtx, &STATECHARTS),
        OwnerLayout::TimeSeries => {
            open_table(wtx, TS_CHUNKS)?;
            open_each(wtx, &TS_ROWS)
        }
        OwnerLayout::Kv => open_table(wtx, KV),
        OwnerLayout::Blob => {
            open_table(wtx, BLOB_CHUNKS)?;
            open_table(wtx, BLOB_OBJECTS)?;
            open_table(wtx, BLOB_REFS)?;
            open_table(wtx, BLOB_UPLOADS)
        }
        OwnerLayout::SemanticIndex => for_each_semantic_table!(open_table, wtx),
    }
}

pub(crate) fn validate_declared_owner_tables(
    rtx: &ReadTransaction,
    layout: OwnerLayout,
) -> Result<(), String> {
    validate_owner_registry_equality()?;
    let result = match layout {
        OwnerLayout::LedgerOnly => Ok(()),
        OwnerLayout::Rbac => validate_table(rtx, RBAC),
        OwnerLayout::Jobs => {
            validate_each(rtx, &JOB_TABLES_BYTES)?;
            validate_each(rtx, &JOB_TABLES_STR)?;
            validate_table(rtx, JOB_META)?;
            validate_table(rtx, READY_PRIORITY)?;
            validate_table(rtx, READY_CAPABILITY)?;
            validate_table(rtx, LEASE_EXPIRY)?;
            validate_table(rtx, ACTIVE_TOTALS)?;
            validate_table(rtx, BY_DEADLINE)?;
            validate_table(rtx, CANCELLATION)
        }
        OwnerLayout::Statechart => validate_each(rtx, &STATECHARTS),
        OwnerLayout::TimeSeries => {
            validate_table(rtx, TS_CHUNKS)?;
            validate_each(rtx, &TS_ROWS)
        }
        OwnerLayout::Kv => validate_table(rtx, KV),
        OwnerLayout::Blob => {
            validate_table(rtx, BLOB_CHUNKS)?;
            validate_table(rtx, BLOB_OBJECTS)?;
            validate_table(rtx, BLOB_REFS)?;
            validate_table(rtx, BLOB_UPLOADS)
        }
        OwnerLayout::SemanticIndex => for_each_semantic_table!(validate_table, rtx),
    };
    result?;
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
    let expected = LEDGER_TABLE_NAMES
        .iter()
        .copied()
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
            OwnerLayout::Kv => $visit!(KV),
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
            }
        }
    }};
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
    let mut names = LEDGER_TABLE_NAMES.to_vec();
    names.extend_from_slice(owner_table_names(layout));
    names.sort_unstable();
    names
}

pub(crate) fn owner_table_names(layout: OwnerLayout) -> &'static [&'static str] {
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
        OwnerLayout::Kv => &["kv"],
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
        ],
    }
}

pub(crate) fn is_mutation_authority_marker(name: &str) -> bool {
    is_known_mutation_table(name) || is_retired_prototype_table(name)
}

pub(crate) fn is_known_mutation_table(name: &str) -> bool {
    LEDGER_TABLE_NAMES.contains(&name)
        || owner_layouts()
            .iter()
            .any(|layout| owner_table_names(*layout).contains(&name))
}

fn validate_owner_registry_equality() -> Result<(), String> {
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

fn owner_layouts() -> [OwnerLayout; 8] {
    [
        OwnerLayout::LedgerOnly,
        OwnerLayout::Rbac,
        OwnerLayout::Jobs,
        OwnerLayout::Statechart,
        OwnerLayout::TimeSeries,
        OwnerLayout::Kv,
        OwnerLayout::Blob,
        OwnerLayout::SemanticIndex,
    ]
}

fn open_each<K, V>(
    wtx: &WriteTransaction,
    tables: &[TableDefinition<'static, K, V>],
) -> Result<(), String>
where
    K: Key + 'static,
    V: Value + 'static,
{
    for table in tables {
        open_table(wtx, *table)?;
    }
    Ok(())
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

fn validate_each<K, V>(
    rtx: &ReadTransaction,
    tables: &[TableDefinition<'static, K, V>],
) -> Result<(), String>
where
    K: Key + 'static,
    V: Value + 'static,
{
    for table in tables {
        validate_table(rtx, *table)?;
    }
    Ok(())
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
