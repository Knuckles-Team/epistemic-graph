//! The registry rows are declared predecessors, their refusal names the
//! operator command, classification is read-only, and every row upgrades a
//! genuine predecessor file to the current layout exactly once. Row bytes
//! across an upgrade are asserted by the operator command's tests and by each
//! upgrade's own tests.

use super::*;
use crate::direct_state::private_local_tempdir;
use crate::owner::domain::KvOwner;
use crate::owner::lineage::BLOB_BEFORE_HOLDERS;
use crate::owner::persisted_layout::create_predecessor_owner_file;
use sha2::{Digest, Sha256};

const KV_UNDECLARED_GENERATION: LayoutPredecessor = LayoutPredecessor {
    layout: OwnerLayout::Kv,
    label: "kv store generation that no lineage declares",
    owner_tables: &["kv"],
    data_lost: "nothing is known about it",
    file_name: "kv.redb",
};

fn identity() -> PhysicalStoreIdentity {
    PhysicalStoreIdentity::new("physical:test:offline-upgrade-registry").unwrap()
}

fn options(path: &Path) -> OfflineUpgradeInspectionOptions {
    OfflineUpgradeInspectionOptions::new(
        path.parent().unwrap().join("offline-upgrade-inspection"),
        256 * 1024 * 1024,
    )
    .unwrap()
}

fn file_digest(path: &Path) -> Vec<u8> {
    Sha256::digest(std::fs::read(path).unwrap()).to_vec()
}

fn ordinary_open(path: &Path, layout: OwnerLayout) -> Result<(), String> {
    crate::kernel::open_physical_with(
        path,
        identity(),
        None,
        layout,
        crate::StoreOpenOptions::default(),
    )
    .map(drop)
}

#[test]
fn every_registered_upgrade_starts_from_one_declared_predecessor() {
    for (index, upgrade) in OFFLINE_STORE_UPGRADES.iter().enumerate() {
        let predecessor = upgrade.predecessor;
        assert!(
            layout_predecessors(predecessor.layout).contains(predecessor),
            "{} is registered but not declared in the lineage",
            predecessor.label
        );
        assert!(
            OFFLINE_STORE_UPGRADES[..index]
                .iter()
                .all(|earlier| earlier.predecessor != predecessor),
            "{} is registered twice",
            predecessor.label
        );
        assert!(offline_upgrades_for(predecessor.layout)
            .any(|candidate| candidate.predecessor == predecessor));
    }
}

#[test]
fn the_refusal_names_the_command_only_where_an_upgrade_is_registered() {
    let dir = tempfile::tempdir().unwrap();
    for (index, upgrade) in OFFLINE_STORE_UPGRADES.iter().enumerate() {
        let predecessor = upgrade.predecessor;
        let path = dir.path().join(format!("registered-{index}"));
        create_predecessor_owner_file(&path, identity(), predecessor).unwrap();
        let before = file_digest(&path);
        let error = ordinary_open(&path, predecessor.layout).unwrap_err();
        assert!(error.starts_with(&predecessor.error_code()), "{error}");
        assert!(error.contains(OFFLINE_UPGRADE_APPLY_COMMAND), "{error}");
        assert!(!error.contains("a fresh store is created"), "{error}");
        assert!(error.ends_with(&predecessor.operator_step()), "{error}");
        assert!(upgrade
            .refusal(&path)
            .ends_with(&predecessor.operator_step()));
        assert_eq!(file_digest(&path), before, "the refusal must not write");
    }
    let path = dir.path().join("unregistered");
    create_predecessor_owner_file(&path, identity(), &BLOB_BEFORE_HOLDERS).unwrap();
    let error = ordinary_open(&path, OwnerLayout::Blob).unwrap_err();
    assert!(error.contains("move blob.redb aside"), "{error}");
    assert!(!error.contains(OFFLINE_UPGRADE_APPLY_COMMAND), "{error}");
}

#[test]
fn classification_reads_each_format_without_writing() {
    let dir = tempfile::tempdir().unwrap();
    let current = dir.path().join("current");
    drop(StorageKernel::create_owner::<KvOwner>(&current, identity(), None).unwrap());
    let registered = dir.path().join("registered");
    create_predecessor_owner_file(&registered, identity(), &AGENT_LIBRARY_BEFORE_MCP_CATALOG)
        .unwrap();
    let unregistered = dir.path().join("unregistered");
    create_predecessor_owner_file(&unregistered, identity(), &BLOB_BEFORE_HOLDERS).unwrap();
    let unknown = dir.path().join("unknown");
    create_predecessor_owner_file(&unknown, identity(), &KV_UNDECLARED_GENERATION).unwrap();
    let before: Vec<_> = [&current, &registered, &unregistered, &unknown]
        .map(|path| file_digest(path))
        .to_vec();

    assert!(matches!(
        classify_owner_store_format(&current),
        Ok(OwnerStoreFormat::Current(OwnerLayout::Kv))
    ));
    assert!(matches!(
        classify_owner_store_format(&registered),
        Ok(OwnerStoreFormat::UpgradeAvailable(upgrade))
            if *upgrade.predecessor == AGENT_LIBRARY_BEFORE_MCP_CATALOG
    ));
    assert!(matches!(
        classify_owner_store_format(&unregistered),
        Ok(OwnerStoreFormat::NoUpgrade(predecessor)) if *predecessor == BLOB_BEFORE_HOLDERS
    ));
    assert!(matches!(
        classify_owner_store_format(&unknown),
        Ok(OwnerStoreFormat::Unknown(OwnerLayout::Kv))
    ));
    assert!(classify_owner_store_format(&dir.path().join("absent")).is_err());

    let after: Vec<_> = [&current, &registered, &unregistered, &unknown]
        .map(|path| file_digest(path))
        .to_vec();
    assert_eq!(before, after, "classification must not write");
}

#[test]
fn every_registered_upgrade_runs_once_and_reaches_the_current_layout() {
    for (index, upgrade) in OFFLINE_STORE_UPGRADES.iter().enumerate() {
        let predecessor = upgrade.predecessor;
        let directory = private_local_tempdir();
        let path = directory.path().join(format!("registered-{index}"));
        create_predecessor_owner_file(&path, identity(), predecessor).unwrap();
        assert!(ordinary_open(&path, predecessor.layout).is_err());

        let inspected = upgrade
            .inspect(&path, identity(), None, options(&path))
            .unwrap();
        let report = inspected.apply().unwrap();

        assert_eq!(report.current_layout_digest, predecessor.layout.digest());
        assert_ne!(report.previous_layout_digest, report.current_layout_digest);
        assert_eq!(
            report.previous_authority_epoch + 1,
            report.current_authority_epoch
        );
        assert!(matches!(
            classify_owner_store_format(&path),
            Ok(OwnerStoreFormat::Current(layout)) if layout == predecessor.layout
        ));
        assert!(upgrade
            .inspect(&path, identity(), None, options(&path))
            .is_err());
        ordinary_open(&path, predecessor.layout).unwrap();
    }
}
