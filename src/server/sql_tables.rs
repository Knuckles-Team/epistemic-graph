//! Owner-scoped durable SQL catalog registry.
//!
//! A served SQL table is subordinate to the verified tenant+principal that created
//! it.  Every owner therefore receives a distinct redb database below the configured
//! engine persistence directory.  The filename is a one-way digest; tenant,
//! principal, graph, and local filesystem details never appear in filenames or
//! errors.  There is no shared-store or temporary fallback.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use eg_query::TableStore;
use sha2::{Digest, Sha256};

use crate::server::access::CarrierAuthority;

const SQL_CATALOG_DIR: &str = "sql-catalog";

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

fn store_path(authority: &CarrierAuthority, persist_dir: &Path) -> PathBuf {
    persist_dir
        .join(SQL_CATALOG_DIR)
        .join(owner_filename(authority))
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

/// Resolve the one durable SQL catalog owned by `authority`.
///
/// `persist_dir` is the already-validated served-engine persistence directory.
/// The registry keeps exactly one redb handle per owner file, which satisfies
/// redb's process-local single-open rule while retaining strict owner isolation.
pub(crate) fn user_table_store(
    authority: &CarrierAuthority,
    persist_dir: Option<&Path>,
) -> Result<TableStore, String> {
    let persist_dir = persist_dir.ok_or_else(|| {
        "owner-scoped SQL catalog requires the configured persistence directory".to_string()
    })?;
    let path = store_path(authority, persist_dir);
    let key = registry_key(&path);
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
    if let Ok(metadata) = std::fs::symlink_metadata(&path) {
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
    let store = TableStore::open(&path)
        .map_err(|_| "owner-scoped SQL catalog could not be opened".to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .map_err(|_| "owner-scoped SQL catalog permissions could not be applied".to_string())?;
    }
    stores.insert(key, store.clone());
    Ok(store)
}

fn context_cache_registry(
) -> &'static Mutex<HashMap<String, std::sync::Arc<eg_query::SqlContextCache>>> {
    static CACHES: OnceLock<Mutex<HashMap<String, std::sync::Arc<eg_query::SqlContextCache>>>> =
        OnceLock::new();
    CACHES.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Resolve the ONE served-path whole-`SessionContext` cache (CONCEPT:EG-KG.query.served-context-cache) for
/// `authority`'s owner-scoped SQL catalog — keyed by the SAME owner-hash registry
/// key [`user_table_store`] uses, so repeated served SQL reads from the SAME
/// tenant+actor reuse the SAME `SqlContextCache` instance (the entire point:
/// amortizing the `SessionContext` build ACROSS requests, not just within one). A
/// fresh, empty cache the first time this owner ever runs a served SQL read.
pub(crate) fn sql_context_cache(
    authority: &CarrierAuthority,
    persist_dir: Option<&Path>,
) -> Result<std::sync::Arc<eg_query::SqlContextCache>, String> {
    let persist_dir = persist_dir.ok_or_else(|| {
        "owner-scoped SQL catalog requires the configured persistence directory".to_string()
    })?;
    let key = registry_key(&store_path(authority, persist_dir));
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
