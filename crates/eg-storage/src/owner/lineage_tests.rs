//! The lineage gate (X11-T1..T5, T8): every layout is pinned, every declared
//! predecessor is refused by name through the real open and the manifest
//! chokepoint, and the operator doc is exactly what the registry renders.

use super::*;
use crate::owner::identity::PhysicalStoreIdentity;
use crate::owner::layout::layout_digest_over;
use crate::owner::persisted_layout::create_predecessor_owner_file;
use crate::owner::registry::owner_table_names;
use redb::ReadableDatabase;
use sha2::{Digest, Sha256};

fn identity() -> PhysicalStoreIdentity {
    PhysicalStoreIdentity::new("physical:test:lineage").unwrap()
}

fn file_digest(path: &Path) -> Vec<u8> {
    Sha256::digest(std::fs::read(path).unwrap()).to_vec()
}

fn predecessor_manifest(predecessor: &LayoutPredecessor) -> OwnerManifest {
    let mut manifest = OwnerManifest::new(identity(), predecessor.layout).unwrap();
    manifest.tables.retain(|table| {
        table.ownership == crate::physical::manifest::TableOwnership::Ledger
            || predecessor.owner_tables.contains(&table.table_id.as_str())
    });
    manifest.layout_digest = layout_digest_over(manifest.layout, &manifest.tables);
    manifest
}

/// X11-T1/T6: changing any layout -- a table, a contract, a logical schema
/// revision -- without re-pinning it fails here, naming the layout.
#[test]
fn every_layout_digest_is_pinned() {
    let drifted: Vec<String> = ALL_LAYOUTS
        .iter()
        .filter(|layout| pinned_layout_digest(**layout) != layout_digest_hex(**layout))
        .map(|layout| {
            format!(
                "OwnerLayout::{layout:?} => \"{}\",",
                layout_digest_hex(*layout)
            )
        })
        .collect();
    assert!(
        drifted.is_empty(),
        "these layouts changed without a lineage declaration; declare the predecessor in \
         layout_predecessors and re-pin:\n{}",
        drifted.join("\n")
    );
}

#[test]
fn graph_shard_before_policy_revisions_matches_the_previous_pin() {
    let predecessor = &GRAPH_SHARD_BEFORE_POLICY_REVISIONS;
    let contracts: Vec<_> =
        crate::owner::contract::expected_table_contracts(OwnerLayout::GraphShard)
            .into_iter()
            .filter(|contract| {
                contract.ownership == crate::physical::manifest::TableOwnership::Ledger
                    || predecessor
                        .owner_tables
                        .contains(&contract.table_id.as_str())
            })
            .collect();
    let digest: String = layout_digest_over(OwnerLayout::GraphShard, &contracts)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    assert_eq!(
        digest,
        "95ef6158378d0aaa4df732937cb0f1e08d6d0d104724de10bacbe110f146dcb6"
    );
}

#[test]
fn the_layout_list_is_complete_and_ordered() {
    for (index, layout) in ALL_LAYOUTS.iter().enumerate() {
        assert_eq!(*layout as usize, index, "{layout:?}");
    }
}

#[test]
fn every_predecessor_is_a_different_table_set_with_a_derived_code() {
    for layout in ALL_LAYOUTS {
        let current = owner_table_names(layout);
        let mut seen = std::collections::BTreeSet::new();
        for predecessor in layout_predecessors(layout) {
            assert_eq!(predecessor.layout, layout);
            assert_ne!(predecessor.owner_tables, current, "{}", predecessor.label);
            assert!(
                seen.insert(predecessor.owner_tables),
                "{}",
                predecessor.label
            );
            assert_eq!(
                predecessor.error_code(),
                format!(
                    "{}_FORMAT_UPGRADE_REQUIRED",
                    layout.canonical_name().to_ascii_uppercase()
                )
            );
        }
    }
}

/// X11-T2/T4: a genuine predecessor file is refused through the real open with
/// its named error and removal step, and its bytes are untouched.
#[test]
fn every_declared_predecessor_file_is_refused_by_name_without_a_write() {
    let dir = tempfile::tempdir().unwrap();
    for layout in ALL_LAYOUTS {
        for (generation, predecessor) in layout_predecessors(layout).iter().enumerate() {
            let path = dir
                .path()
                .join(format!("{}-{generation}.redb", layout.canonical_name()));
            create_predecessor_owner_file(&path, identity(), predecessor).unwrap();
            let before = file_digest(&path);
            let error = crate::kernel::open_physical_with(
                &path,
                identity(),
                None,
                layout,
                crate::StoreOpenOptions::default(),
            )
            .err()
            .expect("a predecessor file must not open");
            assert!(
                error.starts_with(&predecessor.error_code()),
                "{layout:?}: {error}"
            );
            assert!(error.ends_with(&predecessor.operator_step()), "{error}");
            assert_eq!(file_digest(&path), before, "{layout:?}: refusal wrote");
        }
    }
}

/// X11-T5: the manifest chokepoint every open, adoption, classification and
/// restore reads through refuses a predecessor by name too.
#[test]
fn the_manifest_chokepoint_names_every_predecessor() {
    for layout in ALL_LAYOUTS {
        for predecessor in layout_predecessors(layout) {
            let error = validate_against_lineage(&predecessor_manifest(predecessor)).unwrap_err();
            assert!(error.starts_with(&predecessor.error_code()), "{error}");
        }
    }
}

/// X11-T3: a digest that is not in the lineage is UNKNOWN, never a
/// predecessor -- a damaged predecessor must not be mistaken for a genuine one.
#[test]
fn an_undeclared_digest_is_unknown_not_a_predecessor() {
    let mut manifest = predecessor_manifest(&AGENT_LIBRARY_BEFORE_CONNECTOR_PACKS);
    manifest.layout_digest[0] ^= 1;
    let error = validate_against_lineage(&manifest).unwrap_err();
    assert!(error.starts_with("OWNER_STORE_FORMAT_UNKNOWN:"), "{error}");
    let current = OwnerManifest::new(identity(), OwnerLayout::Kv).unwrap();
    assert_eq!(validate_against_lineage(&current), Ok(()));
}

/// X11-T8: the operator doc is exactly what the registry renders.
#[test]
fn the_owner_store_format_doc_is_generated_from_the_registry() {
    let path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/operations/owner-store-formats.md");
    let checked_in = std::fs::read_to_string(&path).unwrap_or_default();
    assert_eq!(
        checked_in,
        render_owner_store_formats(),
        "docs/operations/owner-store-formats.md is stale; regenerate it with \
         `cargo run -p eg-storage --example gen_owner_store_formats`"
    );
}

/// EH-150: a predecessor file that ALSO fails the table census -- a stray
/// undeclared table planted beside its predecessor tables -- is refused by its
/// named manifest error, never by the census. The census defect is real (it
/// fails on its own), so this proves the manifest check runs first and wins.
#[test]
fn the_named_manifest_refusal_wins_over_a_table_census_mismatch() {
    const STRAY: redb::TableDefinition<&str, &[u8]> =
        redb::TableDefinition::new("eh150_stray_undeclared_table");
    let dir = tempfile::tempdir().unwrap();
    let predecessor = &AGENT_LIBRARY_BEFORE_CONNECTOR_PACKS;
    let path = dir.path().join("agent_library.redb");
    create_predecessor_owner_file(&path, identity(), predecessor).unwrap();
    {
        let database = redb::Database::open(&path).unwrap();
        let write = database.begin_write().unwrap();
        write
            .open_table(STRAY)
            .unwrap()
            .insert("planted", &b"x"[..])
            .unwrap();
        write.commit().unwrap();
    }
    let census = {
        let database = redb::Database::open(&path).unwrap();
        let read = database.begin_read().unwrap();
        crate::owner::registry::validate_declared_owner_tables(&read, predecessor.layout)
            .unwrap_err()
    };
    assert!(!census.contains("FORMAT_UPGRADE_REQUIRED"), "{census}");

    // The manifest itself names the predecessor...
    let manifest = predecessor_manifest(predecessor);
    let error = manifest.validate().unwrap_err();
    assert!(error.starts_with(&predecessor.error_code()), "{error}");
    // ...and so does the real open of the doubly-defective file.
    let error = crate::kernel::open_physical_with(
        &path,
        identity(),
        None,
        predecessor.layout,
        crate::StoreOpenOptions::default(),
    )
    .err()
    .expect("a predecessor file must not open");
    assert!(error.starts_with(&predecessor.error_code()), "{error}");
}
