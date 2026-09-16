//! Frozen predecessor admission for the explicit SQL checkpoint upgrade.

use super::super::contract::expected_table_contracts;
use super::super::layout::{layout_domain_tag, OwnerLayout};
use super::super::registry::SQL_SOURCE_CHECKPOINTS;
use crate::physical::incarnation::STORAGE_KERNEL_SCHEMA_VERSION;
use crate::physical::manifest::{hash_table_contract, OwnerManifest, TableContract};
use redb::TableHandle;
use sha2::{Digest, Sha256};

pub(super) const PRE_CHECKPOINT_LAYOUT: [u8; 32] = [
    0xb6, 0xb3, 0xee, 0x31, 0xc3, 0x24, 0x7b, 0xc8, 0x36, 0xd5, 0xfa, 0x39, 0x99, 0x45, 0x61, 0xcf,
    0x7e, 0xb1, 0xa5, 0x10, 0xeb, 0xeb, 0xe1, 0xa1, 0x45, 0xbe, 0x72, 0x3c, 0x6d, 0x2f, 0xc0, 0x16,
];

/// Reuse every current typed contract, subtract exactly the added table, and
/// pin the result to its frozen digest. Future unrelated registry changes
/// cannot silently broaden what this one-time migration accepts.
pub(super) fn predecessor_contracts() -> Result<Vec<TableContract>, String> {
    let contracts: Vec<_> = expected_table_contracts(OwnerLayout::Sql)
        .into_iter()
        .filter(|contract| contract.table_id != SQL_SOURCE_CHECKPOINTS.name())
        .collect();
    if predecessor_digest(&contracts) != PRE_CHECKPOINT_LAYOUT {
        return Err("SQL checkpoint predecessor contract has changed".to_string());
    }
    Ok(contracts)
}

pub(super) fn predecessor_digest(contracts: &[TableContract]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(layout_domain_tag());
    digest.update(OwnerLayout::Sql.canonical_name().as_bytes());
    for contract in contracts {
        hash_table_contract(&mut digest, contract);
    }
    digest.finalize().into()
}

pub(super) fn validate_predecessor(manifest: &OwnerManifest) -> Result<(), String> {
    manifest.physical_identity.validate()?;
    if manifest.schema_version != STORAGE_KERNEL_SCHEMA_VERSION
        || manifest.layout != OwnerLayout::Sql
        || manifest.layout_digest != PRE_CHECKPOINT_LAYOUT
        || manifest.tables != predecessor_contracts()?
    {
        return Err("owner manifest is not the exact SQL checkpoint predecessor".to_string());
    }
    Ok(())
}

pub(super) fn successor(manifest: &OwnerManifest) -> Result<OwnerManifest, String> {
    let mut successor = OwnerManifest::new(manifest.physical_identity.clone(), OwnerLayout::Sql)?;
    successor.authority_epoch = manifest
        .authority_epoch
        .checked_add(1)
        .ok_or_else(|| "SQL checkpoint upgrade authority epoch exhausted".to_string())?;
    successor.validate()?;
    Ok(successor)
}
