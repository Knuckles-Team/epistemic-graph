//! Owner-scoped durable SQL catalog registry, plus (CONCEPT:NE-003) the physical
//! resolution layer for the TENANT-scoped shared catalog that
//! [`crate::server::sql_catalog_acl`] layers ownership, grants, and row-level
//! security on top of.
//!
//! ## Two physical layouts, both opaque
//!
//! * **Principal-isolated layout** ([`user_table_store`]) — a served SQL
//!   table is subordinate to the verified tenant+principal that created it. Every
//!   owner receives a distinct redb database below the configured engine
//!   persistence directory. Callers that require shared tenant access use the
//!   authority-gated layout below instead.
//! * **Tenant-scoped shared layout** ([`tenant_table_store`] /
//!   [`tenant_acl_table_store`]) — ONE physical redb database per TENANT, shared
//!   by every actor in that tenant. [`crate::server::sql_catalog_acl`] is the ONLY
//!   sanctioned caller: it gates every table access through ownership/grant checks
//!   and an optional row-level predicate before handing back rows. Opening the raw
//!   tenant store directly (bypassing that module) reintroduces the exact
//!   ungated-intra-tenant-access hazard NE-003 exists to close — don't do it
//!   outside an authority/admin path that itself performs the equivalent checks.
//!
//! In both layouts the filename is a one-way digest; tenant, principal, graph, and
//! local filesystem details never appear in filenames or errors. There is no
//! shared-store fallback for the principal layout, and no ambient/global fallback
//! for the tenant layout either — every resolution requires a verified
//! [`CarrierAuthority`] and the configured persistence directory.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use eg_query::TableStore;
use sha2::{Digest, Sha256};

use crate::server::access::CarrierAuthority;

const SQL_CATALOG_DIR: &str = "sql-catalog";

/// One carrier-owned native SQL-owner mutation: compiled at the owner scope's
/// current version under the verified carrier's idempotency identity.
pub(crate) struct SqlOwnerMutation<'a> {
    pub(crate) authority: &'a CarrierAuthority,
    /// Idempotency namespace for this kind of owner mutation.
    pub(crate) kind: &'static str,
    pub(crate) scope: &'a str,
    pub(crate) request_id: u64,
    pub(crate) attempt_nonce: Option<eg_types::contract::Nonce>,
    pub(crate) created_at_ms: u64,
}

impl SqlOwnerMutation<'_> {
    /// Hand `compile` the batch context for this mutation.
    pub(crate) fn compile<T>(
        &self,
        store: &TableStore,
        compile: impl FnOnce(crate::server::mutation_batch::CompileBatch<'_>) -> Result<T, String>,
    ) -> Result<T, String> {
        let authority = self.authority;
        let expected = store.mutation_version(authority.tenant_scope(), self.scope)?;
        let batch_id = crate::server::mutation_batch::opaque_idempotency_key_for_context(
            self.kind,
            authority.tenant_scope(),
            self.scope,
            Some(authority.actor_scope()),
            authority.idempotency_key(),
        );
        compile(crate::server::mutation_batch::CompileBatch {
            batch_id: &batch_id,
            request_id: self.request_id,
            attempt_nonce: self.attempt_nonce,
            principal: Some(authority.actor_scope()),
            tenant: authority.tenant_scope(),
            graph: self.scope,
            placement_epoch: 0,
            idempotency_key: authority.idempotency_key(),
            expected_graph_version: Some(expected),
            fencing_token: None,
            created_at_ms: self.created_at_ms,
            default_surface: crate::mutation_batch::MutationSurface::Query,
            authoritative_state: None,
        })
    }
}

fn registry() -> &'static Mutex<HashMap<String, TableStore>> {
    static STORES: OnceLock<Mutex<HashMap<String, TableStore>>> = OnceLock::new();
    STORES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn owner_filename(authority: &CarrierAuthority) -> String {
    let mut digest = Sha256::new();
    digest.update(b"epistemic-graph/sql-catalog-owner\0");
    // Catalog ownership follows the verified effective actor, not the transport's
    // credential principal.  That gives the same tenant+actor one catalog through
    // Method::Sql, pgwire, MySQL, MSSQL, SQLite, and OBDA while delegated actors and
    // cross-tenant actors remain separated.
    digest.update(authority.tenant_scope().as_bytes());
    digest.update([0]);
    digest.update(authority.agent_id().as_bytes());
    format!("{}.redb", hex::encode(digest.finalize()))
}

/// One-way digest of a tenant scope alone (CONCEPT:NE-003) — the physical-file
/// boundary for the shared tenant catalog. A DIFFERENT domain-separation prefix
/// than [`owner_filename`] so the two hash spaces can never collide, even for a
/// tenant whose scope string happens to equal some agent_id's bytes.
fn tenant_filename(tenant_scope: &str, suffix: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"epistemic-graph/sql-catalog-tenant\0");
    digest.update(tenant_scope.as_bytes());
    format!("{}.{suffix}.redb", hex::encode(digest.finalize()))
}

fn store_path(authority: &CarrierAuthority, persist_dir: &Path) -> PathBuf {
    persist_dir
        .join(SQL_CATALOG_DIR)
        .join(owner_filename(authority))
}

/// Physical path of the tenant-shared USER-TABLE catalog (rows + schemas).
fn tenant_table_path(tenant_scope: &str, persist_dir: &Path) -> PathBuf {
    persist_dir
        .join(SQL_CATALOG_DIR)
        .join(tenant_filename(tenant_scope, "tables"))
}

/// Physical path of the tenant-shared ACL catalog (ownership, grants, and RLS
/// declarations — see [`crate::server::sql_catalog_acl`]).
fn tenant_acl_path(tenant_scope: &str, persist_dir: &Path) -> PathBuf {
    persist_dir
        .join(SQL_CATALOG_DIR)
        .join(tenant_filename(tenant_scope, "acl"))
}

fn registry_key(path: &Path) -> String {
    let mut digest = Sha256::new();
    digest.update(b"epistemic-graph/sql-catalog-registry\0");
    digest.update(path.as_os_str().to_string_lossy().as_bytes());
    hex::encode(digest.finalize())
}

pub(crate) fn validate_served_configuration(persist_dir: Option<&Path>) -> std::io::Result<()> {
    if persist_dir.is_some() {
        Ok(())
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "owner-scoped SQL catalogs require the configured persistence directory",
        ))
    }
}

/// Open (or fetch the cached handle for) the redb-backed [`TableStore`] at `path`,
/// creating its parent directory and locking down filesystem permissions first.
/// Shared by every physical-catalog resolver in this module (principal-isolated,
/// tenant-shared data, tenant-shared ACL) so redb's process-local single-open rule
/// is satisfied by construction: this registry is the ONE place in the whole
/// process that ever calls [`TableStore::open`] for a given path.
fn open_or_get(path: &Path, tenant_scope: &str) -> Result<TableStore, String> {
    let key = registry_key(path);
    let parent = path
        .parent()
        .ok_or_else(|| "owner-scoped SQL catalog directory is invalid".to_string())?;
    std::fs::create_dir_all(parent)
        .map_err(|_| "owner-scoped SQL catalog directory is unavailable".to_string())?;
    let parent_metadata = std::fs::symlink_metadata(parent)
        .map_err(|_| "owner-scoped SQL catalog directory is unavailable".to_string())?;
    if parent_metadata.file_type().is_symlink() || !parent_metadata.is_dir() {
        return Err("owner-scoped SQL catalog directory is unavailable".to_string());
    }
    if let Ok(metadata) = std::fs::symlink_metadata(path) {
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err("owner-scoped SQL catalog file is unavailable".to_string());
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))
            .map_err(|_| "owner-scoped SQL catalog permissions could not be applied".to_string())?;
    }

    let mut stores = registry()
        .lock()
        .map_err(|_| "owner-scoped SQL catalog registry is unavailable".to_string())?;
    if let Some(store) = stores.get(&key) {
        return Ok(store.clone());
    }
    // RF-RULING-004: the storage kernel never interprets proof bytes; the
    // composition root decides which principal may serve this file's scopes.
    // This binary is that root, so the SQL catalog authenticates against the
    // SAME `EngineScopeAuthority` every other kernel-owned store here uses --
    // there is exactly one grant authority per process, not one per store.
    let authority = crate::store_authority::process_authority();
    let store = TableStore::open_scoped(
        path,
        tenant_scope,
        crate::store_authority::process_verifier(),
        authority.principal(),
        &authority.proof(),
    )
    .map_err(|_| "owner-scoped SQL catalog could not be opened".to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .map_err(|_| "owner-scoped SQL catalog permissions could not be applied".to_string())?;
    }
    stores.insert(key, store.clone());
    Ok(store)
}

/// Resolve the one durable SQL catalog owned by `authority`.
///
/// `persist_dir` is the already-validated served-engine persistence directory.
/// The registry keeps exactly one redb handle per owner file, which satisfies
/// redb's process-local single-open rule while retaining strict owner isolation.
///
/// Callers choose this principal-isolated store explicitly. Tenant-shared access
/// must instead use [`tenant_table_store`] behind SQL source authorization.
pub(crate) fn user_table_store(
    authority: &CarrierAuthority,
    persist_dir: Option<&Path>,
) -> Result<TableStore, String> {
    let persist_dir = persist_dir.ok_or_else(|| {
        "owner-scoped SQL catalog requires the configured persistence directory".to_string()
    })?;
    open_or_get(
        &store_path(authority, persist_dir),
        authority.tenant_scope(),
    )
}

/// Resolve the ONE durable SQL catalog shared by every actor in `tenant_scope`
/// (CONCEPT:NE-003 — the physical half of item 1: "move the physical catalog
/// boundary from (tenant, agent_id) to tenant alone"). Callers MUST gate access
/// through [`crate::server::sql_catalog_acl`] — this function performs no
/// authorization of its own, exactly like `user_table_store` performs none: the
/// physical-open layer answers "which file", never "is this caller allowed".
pub(crate) fn tenant_table_store(
    tenant_scope: &str,
    persist_dir: &Path,
) -> Result<TableStore, String> {
    open_or_get(&tenant_table_path(tenant_scope, persist_dir), tenant_scope)
}

/// Resolve the tenant-shared ACL catalog (ownership, grants, and RLS declarations)
/// for `tenant_scope` (CONCEPT:NE-003). A SEPARATE physical file from
/// [`tenant_table_store`] so the user-data catalog's schema/row tables never
/// intermix with access-control metadata.
pub(crate) fn tenant_acl_table_store(
    tenant_scope: &str,
    persist_dir: &Path,
) -> Result<TableStore, String> {
    open_or_get(&tenant_acl_path(tenant_scope, persist_dir), tenant_scope)
}

/// The process-local coordination seam owned by the cached tenant ACL handle.
/// Every clone of that handle shares this exact lock; there is no parallel lock
/// registry that could drift from the redb handle registry.
///
/// This does not claim cluster ordering. A caller that requires replicated
/// serialization must reject the ACL snapshot's `LocalOnly` capability.
pub(crate) fn tenant_source_authority_lock(
    tenant_scope: &str,
    persist_dir: &Path,
) -> Result<Arc<RwLock<()>>, String> {
    Ok(tenant_acl_table_store(tenant_scope, persist_dir)?.source_authority_lock())
}

/// Test-only: drop a physical catalog's cached handle from the process registry so
/// a subsequent resolve reopens it from disk — the only way to prove durability
/// (as opposed to in-process cache reuse) without an actual process restart.
/// Panics (via the lock) only on registry poisoning, matching every other
/// registry accessor in this module.
#[cfg(test)]
pub(crate) fn evict_for_test(path: &Path) {
    let key = registry_key(path);
    registry().lock().unwrap().remove(&key);
}

#[cfg(test)]
pub(crate) fn tenant_table_path_for_test(tenant_scope: &str, persist_dir: &Path) -> PathBuf {
    tenant_table_path(tenant_scope, persist_dir)
}

#[cfg(test)]
pub(crate) fn tenant_acl_path_for_test(tenant_scope: &str, persist_dir: &Path) -> PathBuf {
    tenant_acl_path(tenant_scope, persist_dir)
}

fn context_cache_registry(
) -> &'static Mutex<HashMap<String, std::sync::Arc<eg_query::SqlContextCache>>> {
    static CACHES: OnceLock<Mutex<HashMap<String, std::sync::Arc<eg_query::SqlContextCache>>>> =
        OnceLock::new();
    CACHES.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Resolve the ONE served-path whole-`SessionContext` cache (CONCEPT:EG-KG.query.served-context-cache) for
/// `authority`'s tenant-scoped SQL catalog — keyed by the SAME tenant-hash registry
/// key [`tenant_table_store`] uses, so repeated served SQL reads from the SAME
/// tenant reuse the SAME `SqlContextCache` instance (the entire point:
/// amortizing the `SessionContext` build ACROSS requests, not just within one). A
/// fresh, empty cache the first time this owner ever runs a served SQL read.
pub(crate) fn sql_context_cache(
    authority: &CarrierAuthority,
    persist_dir: Option<&Path>,
) -> Result<std::sync::Arc<eg_query::SqlContextCache>, String> {
    let persist_dir = persist_dir.ok_or_else(|| {
        "owner-scoped SQL catalog requires the configured persistence directory".to_string()
    })?;
    let key = registry_key(&tenant_table_path(authority.tenant_scope(), persist_dir));
    let mut caches = context_cache_registry()
        .lock()
        .map_err(|_| "owner-scoped SQL context cache registry is unavailable".to_string())?;
    if let Some(cache) = caches.get(&key) {
        return Ok(cache.clone());
    }
    let cache = std::sync::Arc::new(eg_query::SqlContextCache::new());
    caches.insert(key, cache.clone());
    Ok(cache)
}

#[cfg(test)]
pub(crate) fn test_persist_dir() -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    std::env::temp_dir().join(format!(
        "epistemic-graph-sql-test-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::auth::VerifiedRequestContext;

    fn authority(actor: &str, tenant: &str) -> CarrierAuthority {
        CarrierAuthority::from_verified(&VerifiedRequestContext::verified_for_test_in_tenant(
            actor, tenant,
        ))
        .unwrap()
    }

    #[test]
    fn tenant_and_actor_receive_distinct_opaque_catalogs() {
        let base = test_persist_dir();
        let first = authority("actor-a", "tenant-a");
        let peer = authority("actor-b", "tenant-a");
        let other_tenant = authority("actor-a", "tenant-b");

        let first_store = user_table_store(&first, Some(&base)).unwrap();
        let peer_store = user_table_store(&peer, Some(&base)).unwrap();
        let other_store = user_table_store(&other_tenant, Some(&base)).unwrap();
        let schema = eg_query::TableSchema::new(
            "private_rows",
            vec![eg_query::Column::new(
                "value",
                eg_query::ColumnType::Text,
                false,
                false,
            )],
        );
        first_store.create_table(&schema, false).unwrap();

        assert!(first_store.get_schema("private_rows").unwrap().is_some());
        assert!(peer_store.get_schema("private_rows").unwrap().is_none());
        assert!(other_store.get_schema("private_rows").unwrap().is_none());

        let entries = std::fs::read_dir(base.join(SQL_CATALOG_DIR))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(entries.len(), 3);
        assert!(entries.iter().all(|name| {
            let Some(stem) = name.strip_suffix(".redb") else {
                return false;
            };
            stem.len() == 64 && stem.bytes().all(|byte| byte.is_ascii_hexdigit())
        }));
        assert!(entries
            .iter()
            .all(|name| !name.contains("actor") && !name.contains("tenant")));
    }

    #[test]
    fn missing_persistence_directory_fails_closed() {
        let authority = authority("actor-a", "tenant-a");
        let error = match user_table_store(&authority, None) {
            Ok(_) => panic!("missing persistence directory must fail closed"),
            Err(error) => error,
        };
        assert!(!error.contains("actor-a"));
        assert!(!error.contains("tenant-a"));
        assert!(!error.contains('/') && !error.contains('\\'));
    }

    #[test]
    fn same_verified_actor_shares_catalog_across_native_protocols() {
        let pg = CarrierAuthority::from_verified(
            &VerifiedRequestContext::authenticated_sql_wire_actor(
                "native-test-key",
                "pgwire",
                "actor-a",
            )
            .unwrap(),
        )
        .unwrap();
        let mysql = CarrierAuthority::from_verified(
            &VerifiedRequestContext::authenticated_sql_wire_actor(
                "native-test-key",
                "mysql-wire",
                "actor-a",
            )
            .unwrap(),
        )
        .unwrap();
        assert_ne!(pg.owner_scope(), mysql.owner_scope());
        assert_eq!(owner_filename(&pg), owner_filename(&mysql));
    }

    #[test]
    fn cached_tenant_acl_handle_owns_exactly_one_shared_source_lock() {
        let base = test_persist_dir();
        let first = tenant_acl_table_store("tenant-a", &base).unwrap();
        let second = tenant_acl_table_store("tenant-a", &base).unwrap();
        let exposed = tenant_source_authority_lock("tenant-a", &base).unwrap();
        let other = tenant_acl_table_store("tenant-b", &base).unwrap();

        assert!(Arc::ptr_eq(
            &first.source_authority_lock(),
            &second.source_authority_lock()
        ));
        assert!(Arc::ptr_eq(&first.source_authority_lock(), &exposed));
        assert!(!Arc::ptr_eq(
            &first.source_authority_lock(),
            &other.source_authority_lock()
        ));
    }

    #[cfg(unix)]
    #[test]
    fn catalog_directory_symlink_is_rejected() {
        use std::os::unix::fs::symlink;

        let base = test_persist_dir();
        let outside = test_persist_dir();
        std::fs::create_dir_all(&base).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        symlink(&outside, base.join(SQL_CATALOG_DIR)).unwrap();
        assert!(user_table_store(&authority("actor-a", "tenant-a"), Some(&base)).is_err());
    }
}
