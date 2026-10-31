//! Exact predecessor admission for the offline SQL owner-layout upgrade.

use super::super::contract::expected_table_contracts;
use super::super::layout::{layout_digest_over, OwnerLayout};
use super::super::lineage::{SQL_BEFORE_DURABLE_ANN, SQL_BEFORE_SOURCE_CHECKPOINTS};
use crate::physical::incarnation::STORAGE_KERNEL_SCHEMA_VERSION;
use crate::physical::manifest::{OwnerManifest, TableContract, TableOwnership};

/// Frozen digests of the two real SQL predecessor layouts. They were recorded
/// before source checkpoints and before durable ANN/edge registrations.
pub(super) const PRE_SOURCE_CHECKPOINTS: [u8; 32] = [
    0xb6, 0xb3, 0xee, 0x31, 0xc3, 0x24, 0x7b, 0xc8, 0x36, 0xd5, 0xfa, 0x39, 0x99, 0x45, 0x61, 0xcf,
    0x7e, 0xb1, 0xa5, 0x10, 0xeb, 0xeb, 0xe1, 0xa1, 0x45, 0xbe, 0x72, 0x3c, 0x6d, 0x2f, 0xc0, 0x16,
];
pub(super) const PRE_DURABLE_ANN: [u8; 32] = [
    0x2d, 0x56, 0x2f, 0xab, 0xc7, 0x5b, 0x19, 0xe9, 0xc3, 0x63, 0x09, 0xb1, 0xbd, 0xa7, 0xfc, 0xc9,
    0xa0, 0x50, 0xc6, 0x8a, 0xcb, 0xec, 0x87, 0x6f, 0xc0, 0x69, 0xd3, 0xb5, 0x06, 0xd9, 0x3e, 0x1e,
];

pub(super) fn contracts_for(manifest: &OwnerManifest) -> Result<Vec<TableContract>, String> {
    let current = expected_table_contracts(OwnerLayout::Sql);
    for (predecessor, pinned) in [
        (SQL_BEFORE_SOURCE_CHECKPOINTS, PRE_SOURCE_CHECKPOINTS),
        (SQL_BEFORE_DURABLE_ANN, PRE_DURABLE_ANN),
    ] {
        let contracts: Vec<_> = current
            .iter()
            .filter(|contract| {
                contract.ownership == TableOwnership::Ledger
                    || predecessor
                        .owner_tables
                        .contains(&contract.table_id.as_str())
            })
            .cloned()
            .collect();
        if manifest.layout_digest == pinned
            && layout_digest_over(OwnerLayout::Sql, &contracts) == pinned
            && manifest.tables == contracts
        {
            return Ok(contracts);
        }
    }
    Err("owner manifest is not a supported SQL layout predecessor".to_string())
}

pub(super) fn validate_predecessor(manifest: &OwnerManifest) -> Result<(), String> {
    manifest.physical_identity.validate()?;
    if manifest.schema_version != STORAGE_KERNEL_SCHEMA_VERSION
        || manifest.layout != OwnerLayout::Sql
        || manifest.layout_digest == OwnerLayout::Sql.digest()
    {
        return Err("owner manifest is not an exact SQL layout predecessor".to_string());
    }
    contracts_for(manifest)?;
    Ok(())
}

pub(super) fn successor(manifest: &OwnerManifest) -> Result<OwnerManifest, String> {
    let mut successor = OwnerManifest::new(manifest.physical_identity.clone(), OwnerLayout::Sql)?;
    successor.authority_epoch = manifest
        .authority_epoch
        .checked_add(1)
        .ok_or_else(|| "SQL layout upgrade authority epoch exhausted".to_string())?;
    successor.validate()?;
    Ok(successor)
}
