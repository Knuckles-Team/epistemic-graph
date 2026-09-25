//! Frozen predecessor admission for the explicit SQL checkpoint upgrade.

use super::super::contract::expected_table_contracts;
use super::super::layout::{layout_domain_tag, OwnerLayout};
use super::super::registry::{SQL_ANN_DIRTY, SQL_ANN_GENERATIONS, SQL_SOURCE_CHECKPOINTS};
use crate::physical::incarnation::STORAGE_KERNEL_SCHEMA_VERSION;
use crate::physical::manifest::{hash_table_contract, OwnerManifest, TableContract};
use redb::TableHandle;
use sha2::{Digest, Sha256};

pub(super) const PRE_CHECKPOINT_LAYOUT: [u8; 32] = [
    0xb6, 0xb3, 0xee, 0x31, 0xc3, 0x24, 0x7b, 0xc8, 0x36, 0xd5, 0xfa, 0x39, 0x99, 0x45, 0x61, 0xcf,
    0x7e, 0xb1, 0xa5, 0x10, 0xeb, 0xeb, 0xe1, 0xa1, 0x45, 0xbe, 0x72, 0x3c, 0x6d, 0x2f, 0xc0, 0x16,
];

/// The previously current SQL layout, pinned before adding `__sql_ann_dirty__`.
pub(super) const PRE_ANN_DIRTY_LAYOUT: [u8; 32] = [
    0x2d, 0x56, 0x2f, 0xab, 0xc7, 0x5b, 0x19, 0xe9, 0xc3, 0x63, 0x09, 0xb1, 0xbd, 0xa7, 0xfc, 0xc9,
    0xa0, 0x50, 0xc6, 0x8a, 0xcb, 0xec, 0x87, 0x6f, 0xc0, 0x69, 0xd3, 0xb5, 0x06, 0xd9, 0x3e, 0x1e,
];

// Filled with the inspected 20-table digest before the migration is released.
// The all-zero value fails closed until the hosted digest fixture pins it.
pub(super) const PRE_GENERATION_LAYOUT: [u8; 32] = [0; 32];

/// Reuse every current typed contract, subtract exactly the added table, and
/// pin the result to its frozen digest. Future unrelated registry changes
/// cannot silently broaden what this one-time migration accepts.
pub(super) fn predecessor_contracts() -> Result<Vec<TableContract>, String> {
    let contracts: Vec<_> = expected_table_contracts(OwnerLayout::Sql)
        .into_iter()
        .filter(|contract| {
            contract.table_id != SQL_SOURCE_CHECKPOINTS.name()
                && contract.table_id != SQL_ANN_DIRTY.name()
                && contract.table_id != SQL_ANN_GENERATIONS.name()
        })
        .collect();
    if predecessor_digest(&contracts) != PRE_CHECKPOINT_LAYOUT {
        return Err("SQL checkpoint predecessor contract has changed".to_string());
    }
    Ok(contracts)
}

pub(super) fn pre_ann_dirty_contracts() -> Result<Vec<TableContract>, String> {
    let contracts: Vec<_> = expected_table_contracts(OwnerLayout::Sql)
        .into_iter()
        .filter(|contract| {
            contract.table_id != SQL_ANN_DIRTY.name()
                && contract.table_id != SQL_ANN_GENERATIONS.name()
        })
        .collect();
    if predecessor_digest(&contracts) != PRE_ANN_DIRTY_LAYOUT {
        return Err("SQL ANN dirty predecessor contract has changed".to_string());
    }
    Ok(contracts)
}

pub(super) fn pre_generation_contracts() -> Result<Vec<TableContract>, String> {
    let contracts: Vec<_> = expected_table_contracts(OwnerLayout::Sql)
        .into_iter()
        .filter(|contract| contract.table_id != SQL_ANN_GENERATIONS.name())
        .collect();
    if predecessor_digest(&contracts) != PRE_GENERATION_LAYOUT {
        return Err("SQL ANN generation predecessor contract has changed".to_string());
    }
    Ok(contracts)
}

pub(super) fn contracts_for(manifest: &OwnerManifest) -> Result<Vec<TableContract>, String> {
    match manifest.layout_digest {
        PRE_CHECKPOINT_LAYOUT => predecessor_contracts(),
        PRE_ANN_DIRTY_LAYOUT => pre_ann_dirty_contracts(),
        PRE_GENERATION_LAYOUT => pre_generation_contracts(),
        _ => Err("owner manifest is not a supported SQL layout predecessor".to_string()),
    }
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
        || manifest.tables != contracts_for(manifest)?
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
