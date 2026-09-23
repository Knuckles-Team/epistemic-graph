//! The catalog objects of one table store that a SQL read registers beside the
//! graph relations: durable views, stored functions, SQL/PGQ property graphs,
//! pgvector ANN registrations and the managed-index status rows (EH-352). Read
//! once per statement and handed to the context builder as one value.

use eg_core::index::ManagedIndexStatus;

use super::pgfamily::AnnIndexPlan;
use crate::tables::{PropertyGraphCatalogRecord, StoredFunction, TableStore};

/// The name of the managed-index status relation, `information_schema.eg_index_status`.
pub(crate) const INDEX_STATUS_RELATION: &str = "eg_index_status";

/// One store's catalog objects, as one statement sees them.
#[derive(Default)]
pub(crate) struct StoreCatalog {
    pub(crate) views: Vec<(String, String)>,
    pub(crate) functions: Vec<StoredFunction>,
    pub(crate) property_graphs: Vec<PropertyGraphCatalogRecord>,
    pub(crate) ann_indexes: Vec<AnnIndexPlan>,
    pub(crate) index_status: Vec<ManagedIndexStatus>,
}

impl StoreCatalog {
    /// Every catalog object of `store`; property graphs under `graph_scope`.
    pub(crate) fn read(store: &TableStore, graph_scope: &str) -> Result<Self, String> {
        Ok(Self {
            views: store.list_views()?,
            functions: store.list_functions()?,
            property_graphs: store.list_property_graph_records(graph_scope)?,
            ann_indexes: store.list_ann_indexes()?,
            index_status: store.managed_index_status()?,
        })
    }
}

/// Whether `sql` may read the managed-index status relation. Index state moves
/// without a catalog write, so such a statement never uses a cached context.
/// Over-approximates on purpose: a false positive only skips the cache.
pub(crate) fn reads_index_status(sql: &str) -> bool {
    sql.to_ascii_lowercase().contains(INDEX_STATUS_RELATION)
}
