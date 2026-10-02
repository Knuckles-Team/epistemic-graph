//! Print the persisted owner manifest row and the table census of a freshly
//! created graph-shard owner file, exactly as the checked-out build writes
//! them.
//!
//! This is how the lineage fixtures under `crates/eg-storage/fixtures/lineage/`
//! are produced: check out the commit whose layout is to be recorded, copy
//! this one file into `crates/eg-storage/examples/` if that commit predates it,
//! and run `cargo run -p eg-storage --example dump_graph_shard_manifest`. The
//! output is the fixture file, byte for byte. Only public API that every
//! recorded generation already had is used, so the same source builds at each
//! of them.

use eg_storage::{GraphShardOwner, PhysicalStoreIdentity, StorageKernel};
use redb::{MultimapTableHandle, ReadableDatabase, TableDefinition, TableHandle};

/// The physical table that holds the single owner manifest row.
const OWNER_MANIFEST: TableDefinition<'static, &str, &[u8]> =
    TableDefinition::new("mutation_owner_manifest");

/// The identity every lineage fixture is created under, so two generations'
/// manifests differ only in what the layout itself changed.
const FIXTURE_IDENTITY: &str = "physical:fixture:graph-shard-lineage";

fn text(error: impl std::fmt::Display) -> String {
    error.to_string()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn main() -> Result<(), String> {
    let directory = tempfile::tempdir().map_err(text)?;
    let path = directory.path().join("graph-0.redb");
    let identity = PhysicalStoreIdentity::new(FIXTURE_IDENTITY)?;
    drop(StorageKernel::create_owner::<GraphShardOwner>(
        &path, identity, None,
    )?);

    let database = redb::Database::open(&path).map_err(text)?;
    let read = database.begin_read().map_err(text)?;
    let manifest = read
        .open_table(OWNER_MANIFEST)
        .map_err(text)?
        .get("manifest")
        .map_err(text)?
        .ok_or_else(|| "the owner manifest row is missing".to_string())?
        .value()
        .to_vec();
    let mut tables: Vec<String> = read
        .list_tables()
        .map_err(text)?
        .map(|table| table.name().to_string())
        .collect();
    tables.sort();
    let mut multimap_tables: Vec<String> = read
        .list_multimap_tables()
        .map_err(text)?
        .map(|table| table.name().to_string())
        .collect();
    multimap_tables.sort();

    println!("identity={FIXTURE_IDENTITY}");
    println!("manifest_hex={}", hex(&manifest));
    println!("tables={}", tables.join(","));
    println!("multimap_tables={}", multimap_tables.join(","));
    Ok(())
}
