//! Durable registrations and generations of the user-managed edge-native
//! indexes (EH-351 / EH-352), in the tenant's SQL owner file.
//!
//! * `__sql_edge_indexes__` — `(graph, index name) -> spec bytes`: the index
//!   exists for that graph of this tenant.
//! * `__sql_ann_generations__` — the activated generation, under the key
//!   [`edge_generation_key`], in the same part layout as an ANN generation.
//!
//! The bytes are opaque here; `crate::edge_index` encodes and verifies them.

use eg_storage::SQL_EDGE_INDEXES;
use redb::ReadableTable;

use super::ann_durable::{drop_generations_in, read_generation_in, write_parts_in};
use super::{map_err, StoredGeneration, TableStore};

/// One registered edge index: its graph, its name and its encoded spec.
pub(crate) type EdgeIndexRecord = (String, String, Vec<u8>);

/// The generation-table key of edge index `name` on `graph`. The unit
/// separator cannot occur in an ANN registration key.
pub(crate) fn edge_generation_key(graph: &str, name: &str) -> String {
    format!("edge\u{1f}{graph}\u{1f}{name}")
}

impl TableStore {
    /// Register edge index `name` on `graph` with its encoded spec. `Ok(false)`
    /// when that name is already registered on the graph (nothing changes).
    pub(crate) fn put_edge_index_record(
        &self,
        graph: &str,
        name: &str,
        spec: &[u8],
    ) -> Result<bool, String> {
        self.authority.maintain("put-edge-index", graph, |wtx| {
            let mut indexes = wtx.open_table(SQL_EDGE_INDEXES)?;
            if indexes.get((graph, name)).map_err(map_err)?.is_some() {
                return Ok(false);
            }
            indexes.insert((graph, name), spec).map_err(map_err)?;
            Ok(true)
        })
    }

    /// Every registered edge index of `graph`, by name.
    pub(crate) fn edge_index_records(&self, graph: &str) -> Result<Vec<EdgeIndexRecord>, String> {
        let rtx = self.authority.read()?;
        let indexes = rtx.open_owner_table(SQL_EDGE_INDEXES)?;
        let mut records = Vec::new();
        for entry in indexes.range((graph, "")..).map_err(map_err)? {
            let (key, value) = entry.map_err(map_err)?;
            let (row_graph, name) = key.value();
            if row_graph != graph {
                break;
            }
            records.push((graph.to_string(), name.to_string(), value.value().to_vec()));
        }
        Ok(records)
    }

    /// Drop edge index `name` of `graph` with every generation it owns, in one
    /// write. `Ok(false)` when no such index is registered.
    pub(crate) fn drop_edge_index_record(&self, graph: &str, name: &str) -> Result<bool, String> {
        self.authority.maintain("drop-edge-index", graph, |wtx| {
            let removed = wtx
                .open_table(SQL_EDGE_INDEXES)?
                .remove((graph, name))
                .map_err(map_err)?
                .is_some();
            drop_generations_in(wtx, &edge_generation_key(graph, name))?;
            Ok(removed)
        })
    }

    /// Persist the activated generation of edge index `name` on `graph`,
    /// replacing any earlier one. Fenced: an index dropped meanwhile never
    /// gains a generation.
    pub(crate) fn persist_edge_generation(
        &self,
        graph: &str,
        name: &str,
        stored: &StoredGeneration,
    ) -> Result<(), String> {
        let key = edge_generation_key(graph, name);
        self.authority.maintain("edge-generation", graph, |wtx| {
            if wtx
                .open_table(SQL_EDGE_INDEXES)?
                .get((graph, name))
                .map_err(map_err)?
                .is_none()
            {
                return Err(format!("edge index `{name}` was dropped"));
            }
            drop_generations_in(wtx, &key)?;
            write_parts_in(wtx, &key, stored)
        })
    }

    /// The persisted generation of edge index `name` on `graph`, if any.
    pub(crate) fn edge_generation(
        &self,
        graph: &str,
        name: &str,
    ) -> Result<Option<StoredGeneration>, String> {
        let rtx = self.authority.read()?;
        read_generation_in(&rtx, &edge_generation_key(graph, name))
    }
}
