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

    /// Remove a registered table's catalog entry (W03, GOC-75) — the DropTable /
    /// RenameTable counterpart to [`Self::register`]. `true` if an entry was
    /// removed, `false` if it was already absent.
    pub fn remove(&mut self, namespace: &str, name: &str) -> bool {
        self.tables
            .remove(&(namespace.to_string(), name.to_string()))
            .is_some()
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

/// The caller of a catalog route (EG-UNIFIED-DATA-PLANE-R039.1). Its fields
/// are private: the only constructor reads a verified request envelope, so
/// a principal never comes from unverified client input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogPrincipal {
    tenant: String,
    subject: String,
    scopes: Vec<String>,
}

/// The claims the server tier extracted from a request envelope, and
/// whether it verified the envelope signature.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogEnvelope {
    pub tenant: String,
    pub subject: String,
    pub scopes: Vec<String>,
    pub signature_verified: bool,
}

/// A refused catalog call, with its stable code.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CatalogAuthError {
    /// `CATALOG_UNSIGNED`: the envelope signature was not verified.
    Unsigned,
    /// `CATALOG_PRINCIPAL_INVALID`: blank tenant or subject.
    InvalidPrincipal,
    /// `CATALOG_UNAUTHENTICATED`: no principal on the call.
    Unauthenticated,
    /// `CATALOG_CROSS_TENANT`: the principal's tenant does not own the table.
    CrossTenant,
    /// `CATALOG_SCOPE_MISSING`: the principal lacks the route's scope.
    ScopeMissing(String),
}

impl CatalogAuthError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Unsigned => "CATALOG_UNSIGNED",
            Self::InvalidPrincipal => "CATALOG_PRINCIPAL_INVALID",
            Self::Unauthenticated => "CATALOG_UNAUTHENTICATED",
            Self::CrossTenant => "CATALOG_CROSS_TENANT",
            Self::ScopeMissing(_) => "CATALOG_SCOPE_MISSING",
        }
    }
}

impl CatalogPrincipal {
    /// Build a principal from a verified envelope; refuse an unsigned one or
    /// one with a blank tenant or subject.
    pub fn from_verified_envelope(envelope: &CatalogEnvelope) -> Result<Self, CatalogAuthError> {
        if !envelope.signature_verified {
            return Err(CatalogAuthError::Unsigned);
        }
        if envelope.tenant.trim().is_empty() || envelope.subject.trim().is_empty() {
            return Err(CatalogAuthError::InvalidPrincipal);
        }
        Ok(Self {
            tenant: envelope.tenant.clone(),
            subject: envelope.subject.clone(),
            scopes: envelope.scopes.clone(),
        })
    }

    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    pub fn subject(&self) -> &str {
        &self.subject
    }

    pub fn scopes(&self) -> &[String] {
        &self.scopes
    }
}

/// Authorize one catalog call (EG-UNIFIED-DATA-PLANE-R039.2): refuse an
/// absent principal, a principal of another tenant than the table's, and a
/// principal without `required_scope`.
pub fn authorize_catalog_call(
    principal: Option<&CatalogPrincipal>,
    table_tenant: &str,
    required_scope: &str,
) -> Result<(), CatalogAuthError> {
    let principal = principal.ok_or(CatalogAuthError::Unauthenticated)?;
    if principal.tenant != table_tenant {
        return Err(CatalogAuthError::CrossTenant);
    }
    if !principal.scopes.iter().any(|scope| scope == required_scope) {
        return Err(CatalogAuthError::ScopeMissing(required_scope.to_string()));
    }
    Ok(())
}

#[cfg(test)]
mod principal_tests {
    use super::*;

    fn envelope(verified: bool) -> CatalogEnvelope {
        CatalogEnvelope {
            tenant: "acme".to_string(),
            subject: "svc:trino".to_string(),
            scopes: vec!["catalog.read".to_string()],
            signature_verified: verified,
        }
    }

    // spec: EG-UNIFIED-DATA-PLANE-R039.1
    #[test]
    fn principal_builds_only_from_verified_envelope() {
        let principal = CatalogPrincipal::from_verified_envelope(&envelope(true)).unwrap();
        assert_eq!(principal.tenant(), "acme");
        assert_eq!(principal.subject(), "svc:trino");
        assert_eq!(principal.scopes(), ["catalog.read".to_string()]);
        let unsigned = CatalogPrincipal::from_verified_envelope(&envelope(false)).unwrap_err();
        assert_eq!(unsigned.code(), "CATALOG_UNSIGNED");
        let mut blank = envelope(true);
        blank.tenant = " ".to_string();
        assert_eq!(
            CatalogPrincipal::from_verified_envelope(&blank).unwrap_err(),
            CatalogAuthError::InvalidPrincipal
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R039.2
    #[test]
    fn cross_tenant_call_is_refused() {
        let principal = CatalogPrincipal::from_verified_envelope(&envelope(true)).unwrap();
        assert_eq!(
            authorize_catalog_call(Some(&principal), "acme", "catalog.read"),
            Ok(())
        );
        let error = authorize_catalog_call(Some(&principal), "globex", "catalog.read").unwrap_err();
        assert_eq!(error.code(), "CATALOG_CROSS_TENANT");
        let error = authorize_catalog_call(Some(&principal), "acme", "catalog.write").unwrap_err();
        assert_eq!(error.code(), "CATALOG_SCOPE_MISSING");
    }

    // spec: EG-UNIFIED-DATA-PLANE-R039.2
    #[test]
    fn absent_principal_is_refused() {
        let error = authorize_catalog_call(None, "acme", "catalog.read").unwrap_err();
        assert_eq!(error, CatalogAuthError::Unauthenticated);
        assert_eq!(error.code(), "CATALOG_UNAUTHENTICATED");
    }
}
