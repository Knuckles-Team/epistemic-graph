//! Iceberg-REST catalog stub (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns).
//!
//! The interop seam that lets an external engine *discover* our tables: a minimal
//! in-memory catalog keyed by `(namespace, table)` → metadata location, that renders
//! the handful of Iceberg-REST catalog response bodies a client issues to find and
//! open a table:
//!
//! * `GET /v1/namespaces`            → [`IcebergRestCatalog::list_namespaces`]
//! * `GET /v1/namespaces/{ns}/tables`→ [`IcebergRestCatalog::list_tables`]
//! * `GET /v1/namespaces/{ns}/tables/{t}` (LoadTable) →
//!   [`IcebergRestCatalog::load_table`], returning `metadata-location` (+ inline
//!   metadata) so the client fetches the `metadata.json` this crate wrote.
//!
//! ## What is a stub (documented, per CONCEPT:EG-KG.storage.lsn-as-snapshot-returns)
//! This is the catalog *contents + response shapes*, NOT an HTTP server: wiring these
//! bodies onto an axum route on the engine's server tier is the (documented) seam. It
//! is read-only (no create/commit/drop transactions) and does credential/OAuth-free.
//! That is enough for the LTAP interop story — an external reader lists and locates a
//! table — with the transactional catalog left as a follow-up.

use std::collections::BTreeMap;

use serde_json::{json, Value};

/// One registered table's catalog entry (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns).
#[derive(Clone, Debug, PartialEq)]
pub struct TableEntry {
    pub namespace: String,
    pub name: String,
    /// Object-store location of the current `metadata.json` (what the client fetches).
    pub metadata_location: String,
    /// Optionally the inline `metadata.json` content, returned in a LoadTable response
    /// so a client need not do a second fetch.
    pub metadata_json: Option<String>,
}

/// A minimal in-memory Iceberg-REST catalog (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns).
#[derive(Clone, Debug, Default)]
pub struct IcebergRestCatalog {
    // key: (namespace, name)
    tables: BTreeMap<(String, String), TableEntry>,
}

impl IcebergRestCatalog {
    pub fn new() -> Self {
        IcebergRestCatalog::default()
    }

    /// Register / update a table's metadata location (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns). Called by the
    /// materialization tier after it writes a new `metadata.json`.
    pub fn register(
        &mut self,
        namespace: impl Into<String>,
        name: impl Into<String>,
        metadata_location: impl Into<String>,
        metadata_json: Option<String>,
    ) {
        let ns = namespace.into();
        let nm = name.into();
        self.tables.insert(
            (ns.clone(), nm.clone()),
            TableEntry {
                namespace: ns,
                name: nm,
                metadata_location: metadata_location.into(),
                metadata_json,
            },
        );
    }

    /// Number of registered tables.
    pub fn len(&self) -> usize {
        self.tables.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// `GET /v1/namespaces` response body (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns).
    pub fn list_namespaces(&self) -> Value {
        let mut namespaces: Vec<String> = self.tables.keys().map(|(ns, _)| ns.clone()).collect();
        namespaces.sort();
        namespaces.dedup();
        // Iceberg represents a namespace as a list of levels; ours are single-level.
        let levels: Vec<Value> = namespaces.into_iter().map(|ns| json!([ns])).collect();
        json!({ "namespaces": levels })
    }

    /// `GET /v1/namespaces/{ns}/tables` response body (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns).
    pub fn list_tables(&self, namespace: &str) -> Value {
        let identifiers: Vec<Value> = self
            .tables
            .values()
            .filter(|t| t.namespace == namespace)
            .map(|t| json!({ "namespace": [t.namespace], "name": t.name }))
            .collect();
        json!({ "identifiers": identifiers })
    }

    /// The metadata location for a table, or `None` if unknown (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns).
    pub fn metadata_location(&self, namespace: &str, name: &str) -> Option<&str> {
        self.tables
            .get(&(namespace.to_string(), name.to_string()))
            .map(|t| t.metadata_location.as_str())
    }

    /// `GET /v1/namespaces/{ns}/tables/{t}` (LoadTable) response body, or `None` if the
    /// table is not registered (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns).
    pub fn load_table(&self, namespace: &str, name: &str) -> Option<Value> {
        let entry = self
            .tables
            .get(&(namespace.to_string(), name.to_string()))?;
        let metadata: Value = entry
            .metadata_json
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or(Value::Null);
        Some(json!({
            "metadata-location": entry.metadata_location,
            "metadata": metadata,
            "config": {},
        }))
    }
}
