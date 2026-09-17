use crate::codec::{decode_ledger_record, encode_bounded};
use crate::owner::identity::PhysicalStoreIdentity;
use crate::owner::layout::OwnerLayout;
use crate::physical::manifest::OwnerManifest;
use crate::tables::OWNER_MANIFEST;
use redb::{ReadTransaction, ReadableTable, WriteTransaction};

const MANIFEST_KEY: &str = "manifest";

pub(crate) fn read_current_manifest(rtx: &ReadTransaction) -> Result<OwnerManifest, String> {
    let table = rtx
        .open_table(OWNER_MANIFEST)
        .map_err(|error| error.to_string())?;
    read_manifest(&table)
}

pub(crate) fn validate_manifest_read(
    rtx: &ReadTransaction,
    expected: &PhysicalStoreIdentity,
    layout: OwnerLayout,
) -> Result<OwnerManifest, String> {
    let table = rtx
        .open_table(OWNER_MANIFEST)
        .map_err(|error| error.to_string())?;
    read_exact_manifest(&table, expected, layout)
}

pub(crate) fn validate_manifest_write(
    wtx: &WriteTransaction,
    expected: &PhysicalStoreIdentity,
    layout: OwnerLayout,
) -> Result<OwnerManifest, String> {
    let table = wtx
        .open_table(OWNER_MANIFEST)
        .map_err(|error| error.to_string())?;
    read_exact_manifest(&table, expected, layout)
}

fn read_exact_manifest<T>(
    table: &T,
    expected: &PhysicalStoreIdentity,
    layout: OwnerLayout,
) -> Result<OwnerManifest, String>
where
    T: ReadableTable<&'static str, &'static [u8]>,
{
    let manifest = read_manifest(table)?;
    if manifest.physical_identity != *expected || manifest.layout != layout {
        return Err("mutation owner manifest does not match requested authority".to_string());
    }
    Ok(manifest)
}

pub(crate) fn read_manifest<T>(table: &T) -> Result<OwnerManifest, String>
where
    T: redb::ReadableTable<&'static str, &'static [u8]>,
{
    let manifest = read_manifest_slot(table)?;
    manifest.validate()?;
    Ok(manifest)
}

/// Decode the single manifest row WITHOUT validating it against this build's
/// layouts, so a predecessor file can be identified rather than refused as a
/// generic mismatch. Callers that serve the store use [`read_manifest`];
/// decoding grants no serving authority of its own -- the ordinary reader
/// validates the current contract and an explicit offline upgrader (the SQL
/// checkpoint upgrader, `persisted_layout`'s predecessor-layout refusal)
/// validates its frozen predecessor instead.
pub(crate) fn read_manifest_slot<T>(table: &T) -> Result<OwnerManifest, String>
where
    T: redb::ReadableTable<&'static str, &'static [u8]>,
{
    crate::physical::incarnation::read_single_slot(
        table,
        MANIFEST_KEY,
        "mutation store must contain exactly one owner manifest",
        decode_ledger_record::<OwnerManifest>,
    )?
    .ok_or_else(|| "mutation owner manifest is missing".to_string())
}

pub(crate) fn write_new_manifest(
    wtx: &WriteTransaction,
    manifest: &OwnerManifest,
) -> Result<(), String> {
    let mut table = wtx
        .open_table(OWNER_MANIFEST)
        .map_err(|error| error.to_string())?;
    if table
        .iter()
        .map_err(|error| error.to_string())?
        .next()
        .transpose()
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Err("mutation owner manifest already exists".to_string());
    }
    let bytes = encode_bounded(manifest, "mutation owner manifest")?;
    table
        .insert(MANIFEST_KEY, bytes.as_slice())
        .map_err(|error| error.to_string())?;
    Ok(())
}
