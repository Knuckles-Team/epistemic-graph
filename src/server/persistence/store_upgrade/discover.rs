//! Which owner store files a served data directory holds, and the physical
//! identity the server opens each store kind under.

use std::path::{Path, PathBuf};

use eg_storage::OwnerLayout;

use super::super::durable_stores::{bundled_store_authority, BackupScope, DURABLE_STORES};

/// The physical identity the SQL table store opens every catalog file under.
/// That crate exports no accessor for it, so it is restated here; the SQL
/// catalog test reopens an upgraded catalog through the table store, which
/// fails closed if the two ever differ.
#[cfg(feature = "query")]
pub(super) const SQL_PHYSICAL_STORE: &str = "eg-query:sql-user-tables";

/// One owner store file found under the data directory.
pub(super) struct StoreFile {
    /// Path relative to the data directory, as reported to the operator.
    pub(super) name: String,
    pub(super) path: PathBuf,
    /// The store kind the server opens this file as, when this build declares
    /// one for the file's name.
    pub(super) served_layout: Option<OwnerLayout>,
}

impl StoreFile {
    fn new(data_dir: &Path, path: PathBuf, served_layout: Option<OwnerLayout>) -> Self {
        let name = path
            .strip_prefix(data_dir)
            .unwrap_or(&path)
            .display()
            .to_string();
        Self {
            name,
            path,
            served_layout,
        }
    }
}

/// Every owner store file under `data_dir`, in the order they are reported
/// and upgraded: graph shards by index, the named stores in registry order,
/// then the SQL catalogs by file name.
pub(super) fn discover(data_dir: &Path) -> Result<Vec<StoreFile>, String> {
    let mut stores = shard_files(data_dir)?;
    stores.extend(named_files(data_dir));
    stores.extend(sql_catalog_files(data_dir)?);
    Ok(stores)
}

/// The physical identity the server opens a store of `layout` under. An
/// offline upgrade must expect exactly this identity of the file it admits.
pub(super) fn served_identity(layout: OwnerLayout) -> Option<&'static str> {
    match layout {
        OwnerLayout::GraphShard => Some(crate::redb_store::shard::SHARD_PHYSICAL_STORE),
        #[cfg(feature = "query")]
        OwnerLayout::Sql => Some(SQL_PHYSICAL_STORE),
        _ => DURABLE_STORES
            .iter()
            .filter_map(|store| bundled_store_authority(store.file_name))
            .find(|(_, declared)| *declared == layout)
            .map(|(identity, _)| identity),
    }
}

fn is_regular_file(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.is_file())
}

fn shard_files(data_dir: &Path) -> Result<Vec<StoreFile>, String> {
    Ok(crate::redb_layout::discover_indexed_shards(data_dir)?
        .into_iter()
        .map(|path| StoreFile::new(data_dir, path, Some(OwnerLayout::GraphShard)))
        .collect())
}

fn named_files(data_dir: &Path) -> Vec<StoreFile> {
    DURABLE_STORES
        .iter()
        .filter(|store| !matches!(store.scope, BackupScope::Retired(_)))
        .map(|store| (store.file_name, data_dir.join(store.file_name)))
        .filter(|(_, path)| is_regular_file(path))
        .map(|(file_name, path)| {
            let served = bundled_store_authority(file_name).map(|(_, layout)| layout);
            StoreFile::new(data_dir, path, served)
        })
        .collect()
}

#[cfg(feature = "query")]
fn sql_catalog_files(data_dir: &Path) -> Result<Vec<StoreFile>, String> {
    let directory = data_dir.join(crate::server::sql_tables::SQL_CATALOG_DIR);
    let entries = match std::fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(format!("read SQL catalog directory failed: {error}")),
    };
    let mut paths = Vec::new();
    for entry in entries {
        let path = entry
            .map_err(|error| format!("read SQL catalog entry failed: {error}"))?
            .path();
        if is_regular_file(&path) && path.extension().is_some_and(|kind| kind == "redb") {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths
        .into_iter()
        .map(|path| StoreFile::new(data_dir, path, Some(OwnerLayout::Sql)))
        .collect())
}

#[cfg(not(feature = "query"))]
fn sql_catalog_files(_data_dir: &Path) -> Result<Vec<StoreFile>, String> {
    Ok(Vec::new())
}
